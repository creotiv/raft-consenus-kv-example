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
        deadline = time.time() + timeout
        start_index = self.state.commitIndex
        term = self.state.currentTerm
        if self.state.role != "leader":
            raise HTTPException(421, "NOT_LEADER")

        # 1) wait until NOOP committed
        while (
            self.state.log[self.state.commitIndex].term < term
            or self.state.commitIndex < start_index
        ):
            if time.time() > deadline:
                raise HTTPException(408, "READINDEX_TIMEOUT")
            await asyncio.sleep(0.002)

        read_index = self.state.commitIndex

        # 2) Touch a majority. Checking leader's term and commit index (explicit heartbeat)
        ok = 1
        futs = [
            asyncio.create_task(self._probe_follower(client_id, client, term))
            for client_id, client in self.node.clients.items()
        ]
        done, _ = await asyncio.wait(futs, timeout=timeout)
        ok = 1 + sum(1 for d in done if d.result())

        if self.state.isStateMutated(role="leader", currentTerm=term):
            raise HTTPException(421, "NOT_LEADER")

        if ok < self.node.majority():
            raise HTTPException(409, "READINDEX_NO_QUORUM")

        # 3) Ensure local state machine caught up past the cluster’s commit point
        while self.state.lastApplied < read_index:
            if time.time() > deadline:
                raise HTTPException(408, "READINDEX_TIMEOUT")
            await asyncio.sleep(0.002)

        if self.state.isStateMutated(role="leader", currentTerm=term):
            raise HTTPException(421, "NOT_LEADER")

    async def _probe_follower(self, client_id, client, term):
        try:
            res = await self.replicate_peer(
                client_id, client, heartbeat=True, term=term
            )
            return res
        except Exception as e:
            logger.error(f"probe_follower error: {e}")
            return False
