import asyncio, random, time
from typing import Dict, List, Optional
from raftkv.types import LogEntry, AERequest, AEResponse, RVRequest, RVResponse, Role
from raftkv.logstore import LogStore
from raftkv.storage import MetadataStore
from raftkv.statemachine import KVStateMachine
from raftkv.rpc import RaftRPCClient
from raftkv.readindex import ReadIndex
from raftkv.snapshot import SnapshotStore
from raftkv.raft.election import Election
from raftkv.raft.replication import Replication
from raftkv.raft.rpc import RPC


class RaftNode:
    def __init__(self, cfg):
        self.cfg = cfg
        self.id = cfg["node_id"]
        self.url = f'http://{cfg["host"]}:{cfg["port"]}'
        self.peers_info = cfg["peers"]
        self.clients: Dict[str, RaftRPCClient] = {
            p["id"]: RaftRPCClient(p["url"]) for p in self.peers_info
        }
        self.meta = MetadataStore(cfg["data_dir"])
        self.logstore = LogStore(cfg["data_dir"])
        self.sm = KVStateMachine()
        self.readindex = ReadIndex(self)
        self.election = Election(self)
        self.rpc = RPC(self)
        self.replication = Replication(self)
        # self.snapshot = SnapshotStore(cfg["data_dir"])

        # Persistent (loaded later)
        # Epoch number of election
        self.currentTerm = 0
        self.votedFor: Optional[str] = None
        self.log: List[LogEntry] = [LogEntry(index=1, term=0, cmd=b"")]  # ??? index=1

        # Volatile state
        # Index of the last entry in the log that is durable on all servers
        # and safe to apply to the state machine
        self.commitIndex = 0
        # Index of the last entry that has been applied to the state machine
        self.lastApplied = 0
        self.role: Role = "follower"
        self.leaderId: Optional[str] = None

        # Volatile Leader state
        # Index of the next log entry to send to a peer
        self.nextIndex: Dict[str, int] = {
            p["id"]: self.log[-1].index + 1 for p in self.peers_info
        }
        # Index of the last entry in the log that has been replicated on a peer
        self.matchIndex: Dict[str, int] = {p["id"]: 0 for p in self.peers_info}

        # Timing
        self._bg_tasks: list[asyncio.Task] = []

    async def start(self):
        m = await self.meta.load()
        self.currentTerm = m.get("term", 0)
        self.votedFor = m.get("votedFor", None)
        # snap = await self.snapshot.load()
        self.log = await self.logstore.load()
        # self.sm.kv = snap["kv"]
        # self.lastApplied = snap["lastIncludedIndex"]

        # assert self.log[-1].index >= self.lastApplied, "log truncated before snapshot"

        # # commitIndex remains 0 until leader tells us; we'll advance/apply after election
        # self.commitIndex = max(self.commitIndex, self.lastApplied)

        self._bg_tasks.append(asyncio.create_task(self.apply_loop()))
        await self.election.start()
        await self.replication.start()

        return self

    async def apply_loop(self):
        while True:
            while self.lastApplied < self.commitIndex:
                print(f"#### Apply loop: {self.lastApplied} {self.commitIndex}")
                self.lastApplied += 1
                self.sm.apply(self.log[self.lastApplied].cmd)
            await asyncio.sleep(0.005)

    # async def apply_loop(self):
    #     pending_since_snapshot = 0
    #     while True:
    #         progressed = False
    #         while self.lastApplied < self.commitIndex:
    #             self.lastApplied += 1
    #             self.sm.apply(self.log[self.lastApplied].cmd)
    #             pending_since_snapshot += 1
    #             progressed = True
    #         if (
    #             progressed and pending_since_snapshot >= 64
    #         ):  # snapshot every 64 applied entries (tunable)
    #             # Persist snapshot of applied prefix
    #             last_term = (
    #                 self.log[self.lastApplied].term
    #                 if self.lastApplied < len(self.log)
    #                 else 0
    #             )
    #             await self.snapshot.save(self.lastApplied, last_term, dict(self.sm.kv))
    #             pending_since_snapshot = 0
    #         await asyncio.sleep(0.005)
    #         print("KV", self.sm.kv)

    # --------------- API ---------------
    async def propose(self, cmd: bytes):
        if self.role != "leader":
            raise RuntimeError("NOT_LEADER")
        idx = self.log[-1].index + 1
        entry = LogEntry(index=idx, term=self.currentTerm, cmd=cmd)

        # Append locally first
        await self.logstore.append([entry])
        self.log.append(entry)

        # Replicate
        for _ in range(5):
            await self.replication.replicate([entry])
            if self.commitIndex >= idx:
                return
            await asyncio.sleep(self.heartbeat)
        raise RuntimeError("COMMIT_TIMEOUT")

    async def linearizable_get(self, key: str):
        await self.readindex.barrier()
        return self.sm.get(key)
