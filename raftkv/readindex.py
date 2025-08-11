import asyncio
import time


class ReadIndex:
    def __init__(self, node):
        self.node = node

    async def barrier(self, timeout=1.0):
        if self.node.role != "leader":
            raise RuntimeError("NOT_LEADER")

        term = self.node.currentTerm
        start_idx = self.node.log[-1].index

        # 1) Touch a majority (explicit heartbeat)
        ok = 1  # self counts
        futs = [
            asyncio.create_task(self._probe_follower(client_id, client, term))
            for client_id, client in self.node.clients.items()
        ]
        done, _ = await asyncio.wait(futs, timeout=timeout)
        ok = 1 + sum(1 for d in done if d.result())

        print(f"ok: {ok}")

        if ok < self.node.majority():
            raise RuntimeError("READINDEX_NO_QUORUM")

        # 2) Ensure local state machine caught up past the cluster’s commit point
        deadline = time.time() + timeout
        while self.node.lastApplied < self.node.commitIndex:
            if time.time() > deadline:
                raise RuntimeError("READINDEX_TIMEOUT")
            await asyncio.sleep(0.002)

        # Optional: ensure we didn’t change term mid‑flight
        if self.node.currentTerm != term or self.node.role != "leader":
            raise RuntimeError("NOT_LEADER")

    async def _probe_follower(self, client_id, client, term):
        try:
            # send AppendEntries with entries=[], prev based on that follower’s nextIndex-1
            # (you likely have a helper already)
            res = await client.append_entries(self.node.build_empty_ae_for(client_id))
            print(f"probe_follower: {res}")
            return res.success and res.term == term
        except Exception as e:
            print(f"probe_follower error: {e}")
            return False
