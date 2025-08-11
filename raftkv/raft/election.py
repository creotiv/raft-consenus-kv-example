import time
import random
import asyncio
from typing import Optional
from raftkv.rpc import RaftRPCClient
from raftkv.types import LogEntry, AERequest, AEResponse, RVRequest, RVResponse, Role


class Election:
    def __init__(self, node):
        self.node = node
        self.elec_min = node.cfg.get("election_timeout_ms_min", 300) / 1000
        self.elec_max = node.cfg.get("election_timeout_ms_max", 600) / 1000
        self.heartbeat_sec = node.cfg.get("heartbeat_ms", 120) / 1000
        self._election_deadline = 0
        self._bg_tasks: list[asyncio.Task] = []

    async def start(self):
        self.reset_election_deadline()
        self._bg_tasks.append(asyncio.create_task(self.heartbeat()))

    async def become_follower(self, term: int, leaderId: Optional[str]):
        # additional check
        if term <= self.node.currentTerm:
            return

        self.node.role = "follower"
        self.node.leaderId = leaderId
        self.node.currentTerm = term
        self.node.votedFor = None
        await self.node.meta.save_term_vote(self.node.currentTerm, self.node.votedFor)
        self.reset_election_deadline()

    async def become_leader(self):
        self.node.role = "leader"
        self.node.leaderId = self.node.id
        next_idx = self.node.log[-1].index + 1
        self.node.nextIndex = {pid: next_idx for pid in self.node.clients}
        self.node.matchIndex = {pid: 0 for pid in self.node.clients}
        self.node.matchIndex[self.node.id] = next_idx - 1  # leader has its full log
        # One immediate heartbeat tick
        await self.node.replication.replicate([])

    def majority(self) -> int:
        return (len(self.node.clients) + 1) // 2 + 1

    def reset_election_deadline(self):
        self._election_deadline = time.time() + random.uniform(
            self.elec_min, self.elec_max
        )

    async def start_election(self):
        self.node.role = "candidate"
        self.node.currentTerm += 1
        self.node.votedFor = self.node.id
        last = self.node.log[-1]
        await self.node.meta.save_term_vote(self.node.currentTerm, self.node.votedFor)
        self.reset_election_deadline()

        async def vote_for_me(req: RVRequest, client: RaftRPCClient):
            try:
                return await client.request_vote(req)
            except Exception:
                return None

        req = RVRequest(
            term=self.node.currentTerm,
            candidateId=self.node.id,
            lastLogIndex=last.index,
            lastLogTerm=last.term,
        )

        coros = [vote_for_me(req, c) for _pid, c in self.node.clients.items()]

        votes = 1
        for task in asyncio.as_completed(coros, timeout=self.elec_max):
            res = await task
            if not res:
                continue
            if res.term > self.node.currentTerm:
                await self.become_follower(res.term, None)
                return
            if res.voteGranted:
                votes += 1
            if votes >= self.majority():
                await self.become_leader()
                return

        # If not elected, remain candidate and wait for next timeout
        self.reset_election_deadline()

    async def heartbeat(self):
        while True:
            await asyncio.sleep(self.heartbeat_sec)
            if self.node.role != "leader" and time.time() >= self._election_deadline:
                print(f"#### Start election: {self.node.role}, {self.node.id}")
                await self.start_election()
