import logging, coloredlogs
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from raftkv.config import load_config
from raftkv.node import RaftController
from raftkv.rpc import mount_raft_routes
from app.http_api import mount_client_api
import cProfile, pstats


def create_app() -> FastAPI:
    config_path = os.getenv("CONFIG_PATH")
    if not config_path:
        raise ValueError(
            "CONFIG_PATH environment variable must "
            "be set (e.g., CONFIG_PATH=configs/node1.yaml)"
        )
    cfg = load_config(config_path)

    logging.basicConfig(level=logging.DEBUG)
    coloredlogs.install(
        level="DEBUG", fmt="%(asctime)s  | %(name)s | %(levelname)s # %(message)s"
    )

    # Disable debug logs for httpx and httpcore
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    node = RaftController(cfg)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pr = cProfile.Profile()
        pr.enable()
        await node.start()
        yield
        await node.stop()
        pr.disable()
        ps = pstats.Stats(pr).sort_stats("cumtime")
        ps.print_stats(20)

    app = FastAPI(title=f"raft-kv {cfg['node_id']}", lifespan=lifespan)

    mount_raft_routes(app, node)
    mount_client_api(app, node)
    return app


# Example:
# CONFIG_PATH=configs/node1.yaml uvicorn main:create_app --factory --port 7101
