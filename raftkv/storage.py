import json, os, aiofiles, asyncio
from typing import Optional

from raftkv.interfaces import MetadataStoreInterface


class MetadataStore(MetadataStoreInterface):
    def __init__(self, path: str):
        self.path = path
        self.lock = asyncio.Lock()
        os.makedirs(path, exist_ok=True)
        self.meta_file = os.path.join(path, "meta.json")

    async def shutdown(self):
        # To be sure that all operations are finished and the data is written to disk
        await self.lock.acquire()
        await self.lock.release()

    async def load(self) -> dict:
        async with self.lock:
            if not os.path.exists(self.meta_file):
                return {"term": 0, "votedFor": None, "last_index": 0}
            async with aiofiles.open(self.meta_file, "r") as f:
                return json.loads(await f.read())

    async def save_term_vote(self, term: int, voted_for: Optional[str]):
        async with self.lock:
            tmp = self.meta_file + ".tmp"
            async with aiofiles.open(tmp, "w") as f:
                await f.write(json.dumps({"term": term, "votedFor": voted_for}))
                await f.flush()
                fd = f.fileno()
                await asyncio.to_thread(os.fsync, fd)
            os.replace(tmp, self.meta_file)
