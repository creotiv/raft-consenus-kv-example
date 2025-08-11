from fastapi import APIRouter, FastAPI, HTTPException, Request
import httpx, asyncio
from .types import AERequest, AEResponse, RVRequest, RVResponse


class RaftRPCClient:
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


def mount_raft_routes(app: FastAPI, node):
    router = APIRouter()

    @router.post("/request_vote")
    async def request_vote(req: RVRequest):
        return await node.rpc.handle_request_vote(req)

    @router.post("/append_entries")
    async def append_entries(req: AERequest):
        return await node.rpc.handle_append_entries(req)

    app.include_router(router, prefix="/raft")
