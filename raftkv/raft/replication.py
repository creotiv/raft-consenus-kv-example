import asyncio
from raftkv.rpc import RaftRPCClient
from raftkv.types import LogEntry, AERequest, AEResponse, RVRequest, RVResponse, Role


class Replication:
    def __init__(self, node):
        self.node = node
        self._bg_tasks: list[asyncio.Task] = []
        self._replication_timeout = 3000

    async def start(self):
        self.node.election.reset_election_deadline()
        self._bg_tasks.append(asyncio.create_task(self.heartbeat()))

    async def heartbeat(self):
        while True:
            await asyncio.sleep(self._replication_timeout)
            if self.node.role == "leader":
                # heartbeat to folowers to not start election
                await self.replicate([])

    async def replicate_peer(
        self, client_id: str, client: RaftRPCClient, new_entries: list[LogEntry]
    ) -> AERequest:
        while True:
            next_idx = self.node.nextIndex[client_id]
            prev_idx = next_idx - 1
            prev_term = (
                self.node.log[prev_idx].term if prev_idx < len(self.node.log) else 0
            )
            # slice entries from next_idx
            entries = (
                [e for e in self.node.log if e.index >= next_idx]
                if new_entries == []
                else new_entries
            )
            req = AERequest(
                term=self.node.currentTerm,
                leaderId=self.node.id,
                prevLogIndex=prev_idx,
                prevLogTerm=prev_term,
                entries=entries,
                leaderCommit=self.node.commitIndex,
            )
            try:
                res = await client.append_entries(req)
            except Exception as e:
                print(f"append_entries error: {e}")
                return

            if res.term > self.node.currentTerm:
                print(f"become_follower: {res.term}")
                await self.node.election.become_follower(res.term, None)
                return

            if res.success:
                print(f"success: {client_id} {entries}")
                if entries:
                    self.node.matchIndex[client_id] = entries[-1].index
                    self.node.nextIndex[client_id] = self.node.matchIndex[client_id] + 1
                return
            else:
                print(f"failed: {client_id} {entries}")
                self.node.nextIndex[client_id] = max(
                    1, min(self.node.nextIndex[client_id] - 1, res.nextIndexHint)
                )

    async def replicate(self, new_entries: list[LogEntry]):
        if self.node.role != "leader":
            return
        self.node.election.reset_election_deadline()

        await asyncio.gather(
            *(
                self.replicate_peer(cid, c, new_entries)
                for cid, c in self.node.clients.items()
            )
        )
        await self.advance_commit_index()

    async def advance_commit_index(self):
        if self.node.role != "leader":
            return

        # find highest N such that N>commitIndex, and majority(matchIndex)>=N and log[N].term==currentTerm
        print(f"#### Log difference: {self.node.log[-1].index - self.node.commitIndex}")
        for N in range(self.node.commitIndex + 1, self.node.log[-1].index + 1):

            replicated = 1 + sum(1 for m in self.node.matchIndex.values() if m >= N)
            if (
                replicated >= self.node.election.majority()
                and self.node.log[N].term == self.node.currentTerm
            ):
                print(f"#### SET Commit index: {N}")
                self.node.commitIndex = N
