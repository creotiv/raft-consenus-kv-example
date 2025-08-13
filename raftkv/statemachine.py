from typing import Dict

from raftkv.interfaces import KVStateMachineInterface


class KVStateMachine(KVStateMachineInterface):
    def __init__(self):
        self.kv: Dict[str, str] = {}

    async def shutdown(self):
        pass

    def apply(self, cmd: bytes):
        parts = cmd.decode().split()
        if not parts:
            return
        op = parts[0].upper()
        if op == "SET":
            k, v = parts[1], " ".join(parts[2:])
            self.kv[k] = v
        elif op == "DEL":
            k = parts[1]
            self.kv.pop(k, None)

    def get(self, key: str):
        return self.kv.get(key)
