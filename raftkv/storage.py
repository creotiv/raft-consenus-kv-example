import json, os, aiofiles
from typing import Optional


class MetadataStore:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(path, exist_ok=True)
        self.meta_file = os.path.join(path, "meta.json")

    async def load(self) -> dict:
        if not os.path.exists(self.meta_file):
            return {"term": 0, "votedFor": None, "last_index": 0}
        async with aiofiles.open(self.meta_file, "r") as f:
            return json.loads(await f.read())

    async def save_term_vote(self, term: int, voted_for: Optional[str]):
        tmp = self.meta_file + ".tmp"
        async with aiofiles.open(tmp, "w") as f:
            await f.write(json.dumps({"term": term, "votedFor": voted_for}))
        os.replace(tmp, self.meta_file)
