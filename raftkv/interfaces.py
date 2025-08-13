# raftkv/raft/components.py
from abc import ABC, abstractmethod
from typing import Optional

from raftkv.models import AERequest, AEResponse, RVRequest, RVResponse


class RPCClientInterface(ABC):
    """Base interface for all RPC Clients"""

    @abstractmethod
    async def request_vote(self, req: RVRequest) -> RVResponse:
        pass

    @abstractmethod
    async def append_entries(self, req: AERequest) -> AEResponse:
        pass


class RaftComponentRunnerInterface(ABC):
    """Base interface for all Raft Components"""

    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    async def stop(self):
        pass


class RaftComponentInterface(ABC):
    """Base interface for all Raft Components"""

    @abstractmethod
    async def shutdown(self):
        pass


class RaftNodeInterface(ABC):
    """Base interface for all Raft Node"""

    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    async def stop(self):
        pass

    @abstractmethod
    async def stop_replication(self):
        pass

    @abstractmethod
    async def become_follower(self, term: int, leaderId: Optional[str]):
        pass

    @abstractmethod
    def majority(self) -> int:
        pass

    @abstractmethod
    def reset_election_deadline(self):
        pass

    @abstractmethod
    async def handle_request_vote(self):
        pass

    @abstractmethod
    async def handle_append_entries(self):
        pass

    @abstractmethod
    async def save_term_vote(self, term: int, voted_for: Optional[str]):
        pass

    @abstractmethod
    async def replicate_peer(
        self, client_id: str, client: RPCClientInterface, heartbeat: bool = False
    ) -> bool:
        pass


class ElectionInterface(RaftComponentRunnerInterface):
    """Handles leader election logic"""


class ReplicationInterface(RaftComponentRunnerInterface):
    """Handles log replication"""


class RPCInterface(RaftComponentInterface):
    """Handles incoming RPC requests"""


class ReadIndexInterface(RaftComponentInterface):
    """Handles linearizable reads"""


class LogStoreInterface(RaftComponentInterface):
    """Handles linearizable reads"""


class MetadataStoreInterface(RaftComponentInterface):
    """Handles metadata storage"""


class KVStateMachineInterface(RaftComponentInterface):
    """Handles state machine"""
