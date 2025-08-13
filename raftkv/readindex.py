import asyncio
import time

from fastapi import HTTPException
from raftkv.interfaces import ReadIndexInterface
import logging

logger = logging.getLogger("READINDEX")


class ReadIndex(ReadIndexInterface):
    """
    ReadIndex is a helper class for the leader to make a linearizable read.
    It is used to ensure that the followers are caught up to the commit index.
    It is also used to ensure that the leader is the current leader.
    It gives a strong consistency in reads.
    """

    def __init__(self, node):
        self.node = node
        self.state = node.state

    async def shutdown(self):
        pass

    async def barrier(self, timeout=1.0):
        if self.state.role != "leader":
            raise HTTPException(421, "NOT_LEADER")

        term = self.state.currentTerm

        # 1) Touch a majority (explicit heartbeat)
        ok = 1
        futs = [
            asyncio.create_task(self._probe_follower(client_id, client))
            for client_id, client in self.node.clients.items()
        ]
        done, _ = await asyncio.wait(futs, timeout=timeout)
        ok = 1 + sum(1 for d in done if d.result())

        if ok < self.node.majority():
            raise HTTPException(409, "READINDEX_NO_QUORUM")

        # 2) Ensure local state machine caught up past the cluster’s commit point
        deadline = time.time() + timeout
        while self.state.lastApplied < self.state.commitIndex:
            if time.time() > deadline:
                raise HTTPException(408, "READINDEX_TIMEOUT")
            await asyncio.sleep(0.002)

        # Optional: ensure we didn’t change term mid‑flight
        if self.state.currentTerm != term or self.state.role != "leader":
            raise HTTPException(421, "NOT_LEADER")

    async def _probe_follower(self, client_id, client):
        try:
            res = await self.replicate_peer(client_id, client, heartbeat=True)
            return res
        except Exception as e:
            logger.error(f"probe_follower error: {e}")
            return False
