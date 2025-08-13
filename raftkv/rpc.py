from fastapi import APIRouter, FastAPI, HTTPException
import httpx
import logging

from raftkv.interfaces import RPCClientInterface
from .models import AERequest, AEResponse, RVRequest, RVResponse
from raftkv.models import AERequest, AEResponse, RVRequest, RVResponse

logger = logging.getLogger("RPC")


class RaftRPCClient(RPCClientInterface):
    def __init__(self, base_url: str):
        self.base_url = base_url
        self._client = httpx.AsyncClient(timeout=5.0)

    async def request_vote(self, req: RVRequest) -> RVResponse:
        r = await self._client.post(
            f"{self.base_url}/raft/request_vote", json=req.model_dump(mode="json")
        )
        r.raise_for_status()
        return RVResponse(**r.json())

    async def append_entries(self, req: AERequest) -> AEResponse:
        r = await self._client.post(
            f"{self.base_url}/raft/append_entries", json=req.model_dump(mode="json")
        )
        r.raise_for_status()
        return AEResponse(**r.json())


class RPC:
    def __init__(self, node):
        self.node = node
        self.state = node.state

    async def handle_request_vote(self, req: RVRequest) -> RVResponse:
        if req.term < self.state.currentTerm:
            return RVResponse(term=self.state.currentTerm, voteGranted=False)
        if req.term > self.state.currentTerm:
            await self.node.become_follower(req.term, None)
        up_to_date = (req.lastLogTerm > self.state.log[-1].term) or (
            req.lastLogTerm == self.state.log[-1].term
            and req.lastLogIndex >= self.state.log[-1].index
        )
        can_vote = self.state.votedFor in (None, req.candidateId)
        if can_vote and up_to_date:
            self.state.votedFor = req.candidateId
            await self.node.save_term_vote(self.state.currentTerm, self.state.votedFor)
            self.node.reset_election_deadline()
            return RVResponse(term=self.state.currentTerm, voteGranted=True)
        return RVResponse(term=self.state.currentTerm, voteGranted=False)

    def error_response(
        self, nextIndexHint: int = None, conflictTerm: int = None
    ) -> AEResponse:
        return AEResponse(
            term=self.state.currentTerm,
            success=False,
            nextIndexHint=nextIndexHint or self.state.log[-1].index + 1,
            conflictTerm=conflictTerm,
        )

    async def handle_append_entries(self, req: AERequest) -> AEResponse:
        logger.debug(
            f"Handle append entries: {req.term} {req.prevLogIndex} {req.prevLogTerm} {self.state.currentTerm} {len(req.entries)}"
        )
        # 1) Term check
        if req.term < self.state.currentTerm:
            return self.error_response()

        # If term is greater than current term or role is not follower, become follower
        if req.term > self.state.currentTerm or (
            self.state.role != "follower" and req.leaderId != self.state.leaderId
        ):
            await self.node.become_follower(req.term, req.leaderId)
        else:
            # elif self.state.role == "follower":
            self.state.leaderId = req.leaderId
            self.node.reset_election_deadline()
        # else:
        #     return self.error_response()

        # 2) Prev index/term check (covers “too short” and mismatch)
        last_index = self.state.log[-1].index
        if req.prevLogIndex > last_index:
            return self.error_response(nextIndexHint=last_index + 1)

        if (
            req.prevLogIndex >= 0
            and self.state.log[req.prevLogIndex].term != req.prevLogTerm
        ):
            conflict_term = self.state.log[req.prevLogIndex].term
            conflict_index = req.prevLogIndex
            while (
                conflict_index > 0
                and self.state.log[conflict_index - 1].term == conflict_term
            ):
                conflict_index -= 1
            return self.error_response(
                conflictTerm=conflict_term, nextIndexHint=conflict_index
            )

        # 3) Merge entries (delete only on first conflict)
        i = 0
        while i < len(req.entries):
            idx = req.prevLogIndex + 1 + i
            if idx <= last_index:
                if self.state.log[idx].term != req.entries[i].term:
                    # Safety: never truncate committed entries
                    assert (
                        idx > self.state.commitIndex
                    ), f"Attempt to truncate committed index {idx} <= commitIndex {self.state.commitIndex}"
                    # conflict → truncate from idx
                    await self.node.truncate_log_from(idx)
                    last_index = self.state.log[-1].index
                    break
            else:
                break
            i += 1

        # 4) Append any new tail
        if i < len(req.entries):
            tail = req.entries[i:]
            await self.node.append_to_log(tail)
            last_index = self.state.log[-1].index

        # 5) Advance commit index
        if req.leaderCommit > self.state.commitIndex:
            self.state.commitIndex = min(req.leaderCommit, last_index)
        return AEResponse(
            term=self.state.currentTerm, success=True, nextIndexHint=last_index + 1
        )


def mount_raft_routes(app: FastAPI, node):
    router = APIRouter()

    @router.post("/request_vote")
    async def request_vote(req: RVRequest):
        return await node.rpc.handle_request_vote(req)

    @router.post("/append_entries")
    async def append_entries(req: AERequest):
        return await node.rpc.handle_append_entries(req)

    app.include_router(router, prefix="/raft")
