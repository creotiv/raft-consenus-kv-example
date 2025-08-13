import time
import random
import asyncio
from typing import Optional
from raftkv.interfaces import ElectionInterface
from raftkv.rpc import RaftRPCClient
from raftkv.models import LogEntry, AERequest, AEResponse, RVRequest, RVResponse, Role

import logging

logger = logging.getLogger("ELECTION")


class ElectionManager(ElectionInterface):
    def __init__(self, node):
        self.node = node
        self.state = node.state
        self.shutdown = asyncio.Event()
        self.elec_min = node.cfg.get("election_timeout_ms_min", 300) / 1000
        self.elec_max = node.cfg.get("election_timeout_ms_max", 600) / 1000
        self.heartbeat_sec = node.cfg.get("heartbeat_ms", 120) / 1000
        self._election_deadline = 0
        self._heartbeat_task: asyncio.Task | None = None

    async def start(self):
        self.reset_election_deadline()
        self._heartbeat_task = asyncio.create_task(self.heartbeat())
        logger.info(f"ElectionManager started: {self.state.role}")

    async def stop(self):
        self.shutdown.set()
        self._heartbeat_task.cancel()

    async def become_follower(self, term: int, leaderId: Optional[str]):
        self.state.role = "follower"
        await self.node.stop_replication()
        self.state.leaderId = leaderId
        self.state.currentTerm = term
        self.state.votedFor = None
        await self.node.save_term_vote(self.state.currentTerm, self.state.votedFor)
        self.reset_election_deadline()

    async def become_leader(self):
        self.state.role = "leader"
        self.state.leaderId = self.node.id
        next_idx = self.state.log[-1].index + 1
        self.state.nextIndex = {pid: next_idx for pid in self.node.clients}
        self.state.matchIndex = {pid: 0 for pid in self.node.clients}
        self.state.matchIndex[self.node.id] = next_idx - 1  # leader has its full log
        # One immediate heartbeat with No-op entry
        # This is a no-op entry to ensure:
        # 1. Replicate no-op entry to all followers and update matchIndex (needed to replay log)
        # 2. Reset election deadline
        await self.node.propose(b"NOOP")

    def majority(self) -> int:
        return (len(self.node.clients) + 1) // 2 + 1

    def reset_election_deadline(self):
        self.state.debug(f"Reset election deadline: {time.time()}")
        self._election_deadline = time.time() + random.uniform(
            self.elec_min, self.elec_max
        )

    async def start_election(self):
        self.state.debug(
            f"Starting election for term {self.state.currentTerm + 1} from {self.node.id}"
        )
        minimum_live_nodes = 3
        self.state.role = "candidate"
        await self.node.stop_replication()
        self.state.currentTerm += 1
        self.state.votedFor = self.node.id
        last = self.state.log[-1]
        await self.node.save_term_vote(self.state.currentTerm, self.state.votedFor)
        self.reset_election_deadline()

        async def vote_for_me(req: RVRequest, client: RaftRPCClient):
            try:
                return await client.request_vote(req)
            except Exception as e:
                logger.error(f"Request vote failed: {e}")
                return None

        req = RVRequest(
            term=self.state.currentTerm,
            candidateId=self.node.id,
            lastLogIndex=last.index,
            lastLogTerm=last.term,
        )

        coros = [vote_for_me(req, c) for _pid, c in self.node.clients.items()]
        failed = 0
        votes = 1
        for task in asyncio.as_completed(coros, timeout=self.elec_max):
            res = await task
            if not res:
                failed += 1
                continue
            if res.term > self.state.currentTerm:
                self.state.debug(
                    f"Higher term discovered: {res.term} > {self.state.currentTerm}"
                )
                await self.become_follower(res.term, None)
                return
            if res.voteGranted:
                votes += 1
            # check if the election is won and the minimum live nodes are met
            if (
                votes >= self.majority()
                and (len(self.node.clients) + 1) - failed >= minimum_live_nodes
            ):
                self.state.debug(f"Minimum live nodes: {len(self.node.clients)}")
                self.state.debug(f"Won election with {votes} votes!")
                await self.become_leader()
                return

        # If not elected, remain candidate and wait for next timeout
        self.reset_election_deadline()

    async def heartbeat(self):
        while not self.shutdown.is_set():
            await asyncio.sleep(self.heartbeat_sec)
            if self.state.role != "leader" and time.time() >= self._election_deadline:
                await self.start_election()
