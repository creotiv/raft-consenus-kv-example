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
            "role": node.state.role,
            "term": node.state.currentTerm,
            "leader": node.state.leaderId,
            "commitIndex": node.state.commitIndex,
            "lastApplied": node.state.lastApplied,
        }

    @router.post("/set")
    async def set_value(req: SetReq, response: Response):
        if node.state.role != "leader":
            if node.state.leaderId:
                headers = {"X-Leader": node.state.leaderId}
                for peer in node.peers_info:
                    if peer["id"] == node.state.leaderId:
                        headers["X-URL"] = peer["url"]
                        break
            raise HTTPException(421, "NOT_LEADER", headers=headers)
        await node.set(f"SET {req.key} {req.value}".encode())
        return {"ok": True}

    @router.get("/get/{key}")
    async def get_value(key: str, response: Response):
        if node.state.role != "leader":
            if node.state.leaderId:
                headers = {"X-Leader": node.state.leaderId}
                for peer in node.peers_info:
                    if peer["id"] == node.state.leaderId:
                        headers["X-URL"] = peer["url"]
                        break
            raise HTTPException(421, "NOT_LEADER", headers=headers)
        val = await node.get(key)
        return {"value": val}

    @router.get("/lget/{key}")
    async def get_value(key: str, response: Response):
        if node.state.role != "leader":
            if node.state.leaderId:
                headers = {"X-Leader": node.state.leaderId}
                for peer in node.peers_info:
                    if peer["id"] == node.state.leaderId:
                        headers["X-URL"] = peer["url"]
                        break
            raise HTTPException(421, "NOT_LEADER", headers=headers)
        val = await node.linearizable_get(key)
        return {"value": val}

    @router.delete("/del/{key}")
    async def delete_value(key: str, response: Response):
        if node.state.role != "leader":
            if node.state.leaderId:
                headers = {"X-Leader": node.state.leaderId}
                for peer in node.peers_info:
                    if peer["id"] == node.state.leaderId:
                        headers["X-URL"] = peer["url"]
                        break
            raise HTTPException(421, "NOT_LEADER", headers=headers)
        await node.propose(f"DEL {key}".encode())
        return {"ok": True}

    app.include_router(router, prefix="/kv")
