from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel


def mount_client_api(app, node):
    router = APIRouter()

    class SetReq(BaseModel):
        key: str
        value: str

    @router.get("/health")
    async def health():
        return {
            "role": node.role,
            "term": node.currentTerm,
            "leader": node.leaderId,
            "commitIndex": node.commitIndex,
            "lastApplied": node.lastApplied,
        }

    @router.post("/set")
    async def set_value(req: SetReq, response: Response):
        if node.role != "leader":
            # Best effort hint
            if node.leaderId:
                response.headers["X-Leader"] = node.leaderId
            raise HTTPException(425, "NOT_LEADER")
        await node.propose(f"SET {req.key} {req.value}".encode())
        return {"ok": True}

    @router.get("/get/{key}")
    async def get_value(key: str, response: Response):
        if node.role != "leader":
            if node.leaderId:
                response.headers["X-Leader"] = node.leaderId
            raise HTTPException(425, "NOT_LEADER")
        val = await node.linearizable_get(key)
        return {"value": val}

    @router.delete("/del/{key}")
    async def delete_value(key: str, response: Response):
        if node.role != "leader":
            if node.leaderId:
                response.headers["X-Leader"] = node.leaderId
            raise HTTPException(425, "NOT_LEADER")
        await node.propose(f"DEL {key}".encode())
        return {"ok": True}

    app.include_router(router, prefix="/kv")
