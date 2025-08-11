import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from raftkv.config import load_config
from raftkv.raft.node import RaftNode
from raftkv.rpc import mount_raft_routes
from app.http_api import mount_client_api


def create_app() -> FastAPI:
    config_path = os.getenv("CONFIG_PATH")
    if not config_path:
        raise ValueError(
            "CONFIG_PATH environment variable must "
            "be set (e.g., CONFIG_PATH=configs/node1.yaml)"
        )
    print(f"Loading config from {config_path}")
    cfg = load_config(config_path)
    print(f"Loaded config: {cfg}")
    node = RaftNode(cfg)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await node.start()
        yield

    app = FastAPI(title=f"raft-kv {cfg['node_id']}", lifespan=lifespan)

    mount_raft_routes(app, node)
    mount_client_api(app, node)
    return app


# Example:
# CONFIG_PATH=configs/node1.yaml uvicorn main:create_app --factory --port 7101
