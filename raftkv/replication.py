import logging
import asyncio
from raftkv.interfaces import ReplicationInterface, RPCClientInterface
from raftkv.rpc import RaftRPCClient
from raftkv.models import LogEntry, AERequest, AEResponse, RVRequest, RVResponse, Role

logger = logging.getLogger("REPLICATION")


class Replication(ReplicationInterface):
    def __init__(self, node):
        self.node = node
        self.state = node.state
        self.shutdown = asyncio.Event()
        self.append_event = asyncio.Event()
        self._heartbeat_task: asyncio.Task | None = None
        self._replication_tasks: dict[str, asyncio.Task] = {}
        self.max_batch_bytes = 1024 * 1024
        self.max_batch_entries = 100
        # Use the configured heartbeat interval instead of hardcoded timeout
        self._replication_timeout = node.cfg.get("heartbeat_ms", 120) / 1000

    async def start(self):
        self.node.reset_election_deadline()
        self._heartbeat_task = asyncio.create_task(self.heartbeat())
        for client_id in self.node.clients:
            self._replication_tasks[client_id] = asyncio.create_task(
                self.replication_worker(client_id)
            )
        logger.info(f"ReplicationManager started: {self.state.role}")

    def notify_new_entries(self):
        self.append_event.set()

    async def stop(self):
        self.shutdown.set()
        self._heartbeat_task.cancel()
        for task in self._replication_tasks.values():
            task.cancel()

    async def stop_replication(self):
        self.append_event.clear()

    async def replicate_peer(
        self,
        client_id: str,
        client: RPCClientInterface,
        heartbeat: bool = False,
        term: int = None,
    ) -> bool:
        term = term or self.state.currentTerm
        while True:
            log_entries = []

            next_idx = self.state.nextIndex[client_id]
            end_idx = self.state.log[-1].index
            prev_idx = next_idx - 1

            prev_term = (
                self.state.log[prev_idx].term
                if prev_idx <= self.state.log[-1].index
                else 0
            )

            if not heartbeat:
                log_entries = self._slice_batch(next_idx, end_idx)

            self.state.debug(
                f"Replicate: {client_id} len:{len(log_entries)}, start:{next_idx} end: {end_idx}, pt: {prev_term}"
            )

            req = AERequest(
                term=term,
                leaderId=self.node.id,
                prevLogIndex=prev_idx,
                prevLogTerm=prev_term,
                entries=log_entries,
                leaderCommit=self.state.commitIndex,
            )
            try:
                if self.state.isStateMutated(role="leader"):
                    return False
                res = await client.append_entries(req)
            except Exception as e:
                logger.error(f"append_entries error: {e}")
                return False

            if res.term > self.state.currentTerm:
                await self.node.become_follower(res.term, None)
                return False

            if res.success:
                self.state.debug(f"Success replicated: {client_id} {len(log_entries)}")
                if log_entries:
                    if self.state.isStateMutated(role="leader"):
                        return False
                    last = log_entries[-1].index
                    self.state.matchIndex[client_id] = last
                    self.state.nextIndex[client_id] = last + 1
                return True

            self.state.debug(f"Replication failed: {client_id} {log_entries}")
            if self.state.isStateMutated(role="leader"):
                return False
            self.state.nextIndex[client_id] = max(
                1, min(self.state.nextIndex[client_id] - 1, res.nextIndexHint)
            )

            # TODO: InstallSnapshot
            # If nextIndex moved below snapshot index, switch to InstallSnapshot here
            # if self.node.nextIndex[peer_id] <= self.node.snapshot_last_included_index:
            #     ok = await self.install_snapshot_to(peer_id, client)
            #     if not ok:
            #         return False

    async def replicate(self, heartbeat: bool = False):
        if self.state.role != "leader":
            return
        term = self.state.currentTerm
        self.node.reset_election_deadline()

        await asyncio.gather(
            *(
                self.replicate_peer(cid, c, heartbeat, term)
                for cid, c in self.node.clients.items()
            )
        )
        await self.advance_commit_index()

    async def advance_commit_index(self):
        if self.state.isStateMutated(role="leader"):
            return False
        # find highest N such that N>commitIndex, and majority(matchIndex)>=N and log[N].term==currentTerm
        self.state.debug(
            f"Log difference: {self.state.log[-1].index - self.state.commitIndex}"
        )
        for N in range(self.state.commitIndex + 1, self.state.log[-1].index + 1):
            replicated = 1 + sum(1 for m in self.state.matchIndex.values() if m >= N)
            if (
                replicated >= self.node.majority()
                and self.state.log[N].term == self.state.currentTerm
            ):
                self.state.commitIndex = N
                fut = self.node.pop_waiter(N, None)
                if fut and not fut.done():
                    fut.set_result(True)

    async def heartbeat(self):
        while not self.shutdown.is_set():
            await asyncio.sleep(self._replication_timeout)
            if self.state.role == "leader":
                # heartbeat to followers to not start election
                await self.replicate(heartbeat=True)

    async def replication_worker(self, follower_id):
        while not self.shutdown.is_set():
            await self.append_event.wait()
            self.append_event.clear()

            while True:
                # build a batch from nextIndex[f]..leader_last
                end = self.state.log[-1].index
                term = self.state.currentTerm
                match_idx = self.state.matchIndex[follower_id]
                if match_idx >= end:
                    break

                await self.replicate_peer(
                    follower_id, self.node.clients[follower_id], term=term
                )
                if self.state.isStateMutated(role="leader"):
                    break
                await self.advance_commit_index()

    def _slice_batch(self, start, end):
        size = 0
        batch = []
        for i in range(start, end + 1):
            e = self.state.log[i]
            size += len(e.cmd)
            batch.append(e)
            if len(batch) >= self.max_batch_entries or size >= self.max_batch_bytes:
                break

        return batch
