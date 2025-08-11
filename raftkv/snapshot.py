import json, os, aiofiles


class SnapshotStore:
    def __init__(self, path: str):
        self.path = os.path.join(path, "snapshot.json")
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    async def load(self):
        if not os.path.exists(self.path):
            return {"lastIncludedIndex": 0, "lastIncludedTerm": 0, "kv": {}}
        async with aiofiles.open(self.path, "r") as f:
            return json.loads(await f.read())

    async def save(self, last_idx: int, last_term: int, kv: dict):
        tmp = self.path + ".tmp"
        payload = {
            "lastIncludedIndex": last_idx,
            "lastIncludedTerm": last_term,
            "kv": kv,
        }
        async with aiofiles.open(tmp, "w") as f:
            await f.write(json.dumps(payload))
        os.replace(tmp, self.path)
