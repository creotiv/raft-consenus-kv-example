from typing import Optional, Literal, TypedDict, List
from pydantic import BaseModel

Role = Literal["follower", "candidate", "leader"]


class LogEntry(BaseModel):
    index: int
    term: int
    cmd: bytes  # serialized operation


class AERequest(BaseModel):
    term: int
    leaderId: str
    prevLogIndex: int
    prevLogTerm: int
    entries: list[LogEntry]
    leaderCommit: int


class AEResponse(BaseModel):
    term: int
    success: bool
    nextIndexHint: int
    conflictTerm: Optional[int] = None


class RVRequest(BaseModel):
    term: int
    candidateId: str
    lastLogIndex: int
    lastLogTerm: int


class RVResponse(BaseModel):
    term: int
    voteGranted: bool
