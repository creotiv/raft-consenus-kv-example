import os, struct, aiofiles
from typing import Optional, Tuple
from .types import LogEntry

MAGIC = b"RLOG1"


class LogStore:
    """
    Binary layout:
      MAGIC(5) | [repeated: index(int64), term(int64), len(int32), cmd(bytes)]
    """

    def __init__(self, path: str):
        self.file = os.path.join(path, "log.bin")
        os.makedirs(path, exist_ok=True)
        if not os.path.exists(self.file):
            with open(self.file, "wb") as f:
                f.write(MAGIC)
        self._entries_cache: list[LogEntry] = [LogEntry(index=0, term=0, cmd=b"")]

    async def load(self) -> list[LogEntry]:
        if self._entries_cache and len(self._entries_cache) > 1:
            return self._entries_cache
        entries = [LogEntry(index=0, term=0, cmd=b"")]
        async with aiofiles.open(self.file, "rb") as f:
            if await f.read(5) != MAGIC:
                raise RuntimeError("corrupt log")
            while True:
                hdr = await f.read(8 + 8 + 4)
                if len(hdr) < 20:
                    break
                idx, term, n = struct.unpack(">qqi", hdr)
                cmd = await f.read(n)
                entries.append(LogEntry(index=idx, term=term, cmd=cmd))
        self._entries_cache = entries
        return entries

    async def last(self) -> LogEntry:
        entries = await self.load()
        return entries[-1]

    async def append(self, entries: list[LogEntry]):
        if not entries:
            return
        async with aiofiles.open(self.file, "ab") as f:
            for e in entries:
                await f.write(struct.pack(">qqi", e.index, e.term, len(e.cmd)))
                await f.write(e.cmd)
        self._entries_cache.extend(entries)

    async def truncate_from(self, index: int):
        """Keep entries < index."""
        entries = await self.load()
        keep = [e for e in entries if e.index < index]
        # rewrite
        import tempfile

        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(self.file))
        os.close(fd)
        async with aiofiles.open(tmp, "wb") as f:
            await f.write(MAGIC)
            for e in keep[1:]:
                await f.write(struct.pack(">qqi", e.index, e.term, len(e.cmd)))
                await f.write(e.cmd)
        os.replace(tmp, self.file)
        self._entries_cache = keep
