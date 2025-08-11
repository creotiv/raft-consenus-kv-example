import asyncio, random, time
from typing import Dict, List, Optional
from .types import LogEntry, AERequest, AEResponse, RVRequest, RVResponse, Role
from .logstore import LogStore
from .storage import MetadataStore
from .statemachine import KVStateMachine
from .rpc import RaftRPCClient
from .readindex import ReadIndex
from .snapshot import SnapshotStore


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
        self.snapshot = SnapshotStore(cfg["data_dir"])

        # Persistent (loaded later)
        self.currentTerm = 0
        self.votedFor: Optional[str] = None
        self.log: List[LogEntry] = [LogEntry(index=0, term=0, cmd=b"")]

        # Volatile
        self.commitIndex = 0
        self.lastApplied = 0
        self.role: Role = "follower"
        self.leaderId: Optional[str] = None

        # Leader state
        self.nextIndex: Dict[str, int] = {}
        self.matchIndex: Dict[str, int] = {}

        # Timing
        self.elec_min = cfg.get("election_timeout_ms_min", 300) / 1000
        self.elec_max = cfg.get("election_timeout_ms_max", 600) / 1000
        self.heartbeat = cfg.get("heartbeat_ms", 120) / 1000
        self._election_deadline = 0.0

        self._bg_tasks: list[asyncio.Task] = []

    async def start(self):
        m = await self.meta.load()
        self.currentTerm = m.get("term", 0)
        self.votedFor = m.get("votedFor", None)
        snap = await self.snapshot.load()
        self.log = await self.logstore.load()
        self.sm.kv = snap["kv"]
        self.lastApplied = snap["lastIncludedIndex"]

        assert self.log[-1].index >= self.lastApplied, "log truncated before snapshot"

        # commitIndex remains 0 until leader tells us; we'll advance/apply after election
        self.commitIndex = max(self.commitIndex, self.lastApplied)

        self.reset_election_deadline()
        self._bg_tasks.append(asyncio.create_task(self.tick_loop()))
        self._bg_tasks.append(asyncio.create_task(self.apply_loop()))
        return self

    # --------------- Timers ---------------
    def reset_election_deadline(self):
        self._election_deadline = time.time() + random.uniform(
            self.elec_min, self.elec_max
        )

    async def tick_loop(self):
        while True:
            await asyncio.sleep(self.heartbeat)
            if self.role == "leader":
                # heartbeats
                await self.broadcast_append_entries([])
            elif time.time() >= self._election_deadline:
                await self.start_election()

    # --------------- Elections ---------------
    async def start_election(self):
        self.role = "candidate"
        self.currentTerm += 1
        self.votedFor = self.id
        await self.meta.save_term_vote(self.currentTerm, self.votedFor)
        self.reset_election_deadline()
        votes = 1
        last = self.log[-1]
        req = RVRequest(
            term=self.currentTerm,
            candidateId=self.id,
            lastLogIndex=last.index,
            lastLogTerm=last.term,
        )

        async def solicit(peer_id, client):
            try:
                return await client.request_vote(req)
            except Exception:
                return None

        coros = [solicit(pid, c) for pid, c in self.clients.items()]
        for task in asyncio.as_completed(coros, timeout=self.elec_max):
            res = await task
            if not res:
                continue
            if res.term > self.currentTerm:
                await self.become_follower(res.term, None)
                return
            if res.voteGranted:
                votes += 1
            if votes >= self.majority():
                await self.become_leader()
                return
        # If not elected, remain candidate and wait for next timeout
        self.reset_election_deadline()

    async def become_follower(self, term: int, leaderId: Optional[str]):
        self.role = "follower"
        self.leaderId = leaderId
        if term > self.currentTerm:
            self.currentTerm = term
            self.votedFor = None
            await self.meta.save_term_vote(self.currentTerm, self.votedFor)
        self.reset_election_deadline()

    async def become_leader(self):
        self.role = "leader"
        self.leaderId = self.id
        next_idx = self.log[-1].index + 1
        self.nextIndex = {pid: next_idx for pid in self.clients}
        self.matchIndex = {pid: 0 for pid in self.clients}
        self.matchIndex[self.id] = next_idx - 1  # leader has its full log
        # One immediate heartbeat tick
        await self.broadcast_append_entries([])

    def majority(self) -> int:
        return (len(self.clients) + 1) // 2 + 1

    # --------------- RPC Handlers ---------------
    async def handle_request_vote(self, req: RVRequest) -> RVResponse:
        if req.term < self.currentTerm:
            return RVResponse(term=self.currentTerm, voteGranted=False)
        if req.term > self.currentTerm:
            await self.become_follower(req.term, None)
        up_to_date = (req.lastLogTerm > self.log[-1].term) or (
            req.lastLogTerm == self.log[-1].term
            and req.lastLogIndex >= self.log[-1].index
        )
        can_vote = self.votedFor in (None, req.candidateId)
        if can_vote and up_to_date:
            self.votedFor = req.candidateId
            await self.meta.save_term_vote(self.currentTerm, self.votedFor)
            self.reset_election_deadline()
            return RVResponse(term=self.currentTerm, voteGranted=True)
        return RVResponse(term=self.currentTerm, voteGranted=False)

    async def handle_append_entries(self, req: AERequest) -> AEResponse:
        print(f"handle_append_entries: {req.term} {self.currentTerm} {req.entries}")
        if req.term < self.currentTerm:
            return AEResponse(
                term=self.currentTerm,
                success=False,
                nextIndexHint=self.log[-1].index + 1,
            )
        await self.become_follower(req.term, req.leaderId)

        if req.prevLogIndex > self.log[-1].index:
            return AEResponse(
                term=self.currentTerm,
                success=False,
                nextIndexHint=self.log[-1].index + 1,
            )

        if req.prevLogIndex >= 0 and self.log[req.prevLogIndex].term != req.prevLogTerm:
            # conflict: binary-search optimization could be added here
            return AEResponse(
                term=self.currentTerm,
                success=False,
                nextIndexHint=max(1, req.prevLogIndex),
            )

        # append/overwrite
        # delete conflicting entries
        keep_upto = req.prevLogIndex + 1
        if keep_upto <= self.log[-1].index:
            await self.logstore.truncate_from(keep_upto)
            self.log = self.log[:keep_upto]
        # append new
        if req.entries:
            await self.logstore.append(req.entries)
            self.log.extend(req.entries)

        if req.leaderCommit > self.commitIndex:
            self.commitIndex = min(req.leaderCommit, self.log[-1].index)
        return AEResponse(
            term=self.currentTerm, success=True, nextIndexHint=self.log[-1].index + 1
        )

    # --------------- Replication ---------------
    def build_empty_ae_for(self, client_id: str) -> AERequest:
        return AERequest(
            term=self.currentTerm,
            leaderId=self.id,
            prevLogIndex=self.nextIndex[client_id] - 1,
            prevLogTerm=self.log[self.nextIndex[client_id] - 1].term,
            entries=[],
            leaderCommit=self.commitIndex,
        )

    async def broadcast_append_entries(self, new_entries: list[LogEntry]):
        if self.role != "leader":
            return
        self.reset_election_deadline()
        last = self.log[-1]

        async def to_peer(pid, client):
            next_idx = self.nextIndex[pid]
            prev_idx = next_idx - 1
            prev_term = self.log[prev_idx].term if prev_idx < len(self.log) else 0
            # slice entries from next_idx
            entries = (
                [e for e in self.log if e.index >= next_idx]
                if new_entries == []
                else new_entries
            )
            req = AERequest(
                term=self.currentTerm,
                leaderId=self.id,
                prevLogIndex=prev_idx,
                prevLogTerm=prev_term,
                entries=entries,
                leaderCommit=self.commitIndex,
            )
            try:
                res = await client.append_entries(req)
            except Exception as e:
                print(f"append_entries error: {e}")
                return
            if res.term > self.currentTerm:
                print(f"become_follower: {res.term}")
                await self.become_follower(res.term, None)
                return
            if res.success:
                print(f"success: {pid} {entries}")
                if entries:
                    self.matchIndex[pid] = entries[-1].index
                    self.nextIndex[pid] = self.matchIndex[pid] + 1
            else:
                print(f"failed: {pid} {entries}")
                self.nextIndex[pid] = max(
                    1, min(self.nextIndex[pid] - 1, res.nextIndexHint)
                )

        await asyncio.gather(*(to_peer(pid, c) for pid, c in self.clients.items()))
        await self.advance_commit_index()

    async def advance_commit_index(self):
        if self.role != "leader":
            return
        # find highest N such that N>commitIndex, and majority(matchIndex)>=N and log[N].term==currentTerm
        for N in range(self.commitIndex + 1, self.log[-1].index + 1):
            replicated = 1 + sum(1 for m in self.matchIndex.values() if m >= N)
            if replicated >= self.majority() and self.log[N].term == self.currentTerm:
                self.commitIndex = N

    # --------------- Apply loop ---------------
    async def apply_loop(self):
        pending_since_snapshot = 0
        while True:
            progressed = False
            while self.lastApplied < self.commitIndex:
                self.lastApplied += 1
                self.sm.apply(self.log[self.lastApplied].cmd)
                pending_since_snapshot += 1
                progressed = True
            if (
                progressed and pending_since_snapshot >= 64
            ):  # snapshot every 64 applied entries (tunable)
                # Persist snapshot of applied prefix
                last_term = (
                    self.log[self.lastApplied].term
                    if self.lastApplied < len(self.log)
                    else 0
                )
                await self.snapshot.save(self.lastApplied, last_term, dict(self.sm.kv))
                pending_since_snapshot = 0
            await asyncio.sleep(0.005)
            print("KV", self.sm.kv)

    # --------------- Client ops ---------------
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
            await self.broadcast_append_entries([entry])
            if self.commitIndex >= idx:
                return
            await asyncio.sleep(self.heartbeat)
        raise RuntimeError("COMMIT_TIMEOUT")

    async def linearizable_get(self, key: str):
        await self.readindex.barrier()
        return self.sm.get(key)
