import asyncio
from typing import Dict, List, Optional
import logging

from fastapi import HTTPException
from raftkv.models import LogEntry, Role
from raftkv.logstore import LogStore
from raftkv.interfaces import *
from raftkv.state import StateController
from raftkv.storage import MetadataStore
from raftkv.statemachine import KVStateMachine
from raftkv.rpc import RaftRPCClient
from raftkv.readindex import ReadIndex
from raftkv.election import ElectionManager
from raftkv.replication import Replication
from raftkv.rpc import RPC
from pydantic import BaseModel

logger = logging.getLogger("NODE")


class RaftState(BaseModel):
    log: List[LogEntry]
    currentTerm: int
    votedFor: Optional[str]

    commitIndex: int
    lastApplied: int
    role: Role
    leaderId: Optional[str]

    nextIndex: Dict[str, int]
    matchIndex: Dict[str, int]


class RaftController(RaftNodeInterface):
    def __init__(self, cfg):
        self.cfg = cfg
        self.id = cfg["node_id"]
        self.peers_info = cfg["peers"]
        self.heartbeat_sec = cfg.get("heartbeat_ms", 120) / 1000

        self.clients: Dict[str, RaftRPCClient] = {
            p["id"]: RaftRPCClient(p["url"]) for p in self.peers_info
        }

        self.state = StateController(
            RaftState(
                log=[],
                currentTerm=0,
                votedFor=None,
                commitIndex=0,
                lastApplied=0,
                role="follower",
                leaderId=None,
                nextIndex={p["id"]: 1 for p in self.peers_info},
                matchIndex={p["id"]: 0 for p in self.peers_info},
            )
        )
        self.election = ElectionManager(self)
        self.meta = MetadataStore(cfg["data_dir"])
        self.logstore = LogStore(cfg["data_dir"])
        self.sm = KVStateMachine()
        self.readindex = ReadIndex(self)
        self.rpc = RPC(self)
        self.replication = Replication(self)

        self._commit_waiters = {}
        self._inbox = asyncio.Queue()
        self._bg_tasks: list[asyncio.Task] = []

    async def start(self):
        m = await self.meta.load()
        self.state.currentTerm = m.get("term", 0)
        self.state.votedFor = m.get("votedFor", None)
        logger.info("Loading log from disk...")
        self.state.log = (await self.logstore.load()).copy()
        logger.info(f"Loaded {len(self.state.log)} log entries")

        self._bg_tasks.append(asyncio.create_task(self.apply_loop()))
        self._bg_tasks.append(asyncio.create_task(self.append_loop()))
        await self.election.start()
        await self.replication.start()
        return self

    async def stop(self):
        await self.replication.stop()
        await self.election.stop()
        for task in self._bg_tasks:
            task.cancel()
        self._bg_tasks.clear()

    async def apply_loop(self):
        while True:
            while self.state.lastApplied < self.state.commitIndex:
                self.state.lastApplied += 1
                self.sm.apply(self.state.log[self.state.lastApplied].cmd)
            # TODO: Use event check instead of sleep
            await asyncio.sleep(0.5)

    async def append_loop(self):
        while True:
            term0, cmd0, fut0 = await self._inbox.get()
            batch = [(term0, cmd0, fut0)]
            # small batching window
            try:
                while True:
                    batch.append(self._inbox.get_nowait())
            except asyncio.QueueEmpty:
                pass

            # abort batch if we lost leadership
            if self.state.isStateMutated(role="leader", currentTerm=batch[0][0]):
                for _, _, fut in batch:
                    if not fut.done():
                        fut.set_exception(HTTPException(421, "NOT_LEADER"))
                continue

            # assign indexes & build entries
            start = self.state.log[-1].index + 1
            entries = []
            for i, (term_snap, cmd, fut) in enumerate(batch):
                entries.append(LogEntry(index=start + i, term=term_snap, cmd=cmd))
                self._commit_waiters[start + i] = fut

            # append+fsync once, then update memory
            await self.logstore.append(entries)
            # recheck leadership/term after await
            if self.state.isStateMutated(role="leader", currentTerm=entries[0].term):
                # optional: truncate tail if still present
                for idx in range(entries[0].index, entries[-1].index + 1):
                    fut = self._commit_waiters.pop(idx, None)
                    if fut and not fut.done():
                        fut.set_exception(RuntimeError("LOST_LEADERSHIP"))
                continue

            self.state.log.extend(entries)
            self.replication.notify_new_entries()

    # --------------- Internal API ---------------
    async def stop_replication(self):
        await self.replication.stop_replication()

    async def become_follower(self, term: int, leaderId: Optional[str]):
        await self.election.become_follower(term, leaderId)

    def majority(self) -> int:
        return self.election.majority()

    def reset_election_deadline(self):
        self.election.reset_election_deadline()

    async def handle_request_vote(self):
        await self.election.handle_request_vote()

    async def handle_append_entries(self):
        await self.election.handle_append_entries()

    async def replicate_peer(
        self,
        client_id: str,
        client: RaftRPCClient,
        heartbeat: bool = False,
        term: int = None,
    ) -> bool:
        return await self.replication.replicate_peer(
            client_id, client, heartbeat, term=term
        )

    async def save_term_vote(self, term: int, voted_for: Optional[str]):
        await self.meta.save_term_vote(term, voted_for)

    def pop_waiter(self, index: int, fut: Optional[asyncio.Future]):
        return self._commit_waiters.pop(index, fut)

    async def truncate_log_from(self, index: int):
        await self.logstore.truncate_from(index)
        self.state.log = self.state.log[:index]

    async def append_to_log(self, entries: list[LogEntry]):
        await self.logstore.append(entries)
        self.state.extend("log", entries)

    async def propose(self, cmd: bytes):
        if self.state.role != "leader":
            raise HTTPException(421, "NOT_LEADER")
        fut = asyncio.get_event_loop().create_future()
        await self._inbox.put((self.state.currentTerm, cmd, fut))
        return fut  # resolved when committed

    # --------------- API ---------------
    async def set(self, cmd: bytes):
        if len(self._commit_waiters) > 500:
            raise HTTPException(429, "SYSTEM_OVERLOADED")
        await self.propose(cmd)

    async def get(self, key: str):
        return self.sm.get(key)

    async def linearizable_get(self, key: str):
        await self.readindex.barrier()
        return self.sm.get(key)
