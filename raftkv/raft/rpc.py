from raftkv.types import AERequest, AEResponse, RVRequest, RVResponse


class RPC:
    def __init__(self, node):
        self.node = node

    async def handle_request_vote(self, req: RVRequest) -> RVResponse:
        if req.term < self.node.currentTerm:
            return RVResponse(term=self.node.currentTerm, voteGranted=False)
        if req.term > self.node.currentTerm:
            await self.node.election.become_follower(req.term, None)
        up_to_date = (req.lastLogTerm > self.node.log[-1].term) or (
            req.lastLogTerm == self.node.log[-1].term
            and req.lastLogIndex >= self.node.log[-1].index
        )
        can_vote = self.node.votedFor in (None, req.candidateId)
        if can_vote and up_to_date:
            self.node.votedFor = req.candidateId
            await self.node.meta.save_term_vote(
                self.node.currentTerm, self.node.votedFor
            )
            self.node.election.reset_election_deadline()
            return RVResponse(term=self.node.currentTerm, voteGranted=True)
        return RVResponse(term=self.node.currentTerm, voteGranted=False)

    async def handle_append_entries(self, req: AERequest) -> AEResponse:
        print(
            f"handle_append_entries: {req.term} {self.node.currentTerm} {req.entries}"
        )
        if req.term < self.node.currentTerm:
            return AEResponse(
                term=self.node.currentTerm,
                success=False,
                nextIndexHint=self.node.log[-1].index + 1,
            )
        await self.node.election.become_follower(req.term, req.leaderId)

        if req.prevLogIndex > self.node.log[-1].index:
            return AEResponse(
                term=self.node.currentTerm,
                success=False,
                nextIndexHint=self.node.log[-1].index + 1,
            )

        if (
            req.prevLogIndex >= 0
            and self.node.log[req.prevLogIndex].term != req.prevLogTerm
        ):
            # conflict: binary-search optimization could be added here
            return AEResponse(
                term=self.node.currentTerm,
                success=False,
                nextIndexHint=max(1, req.prevLogIndex),
            )

        # append/overwrite
        # delete conflicting entries
        keep_upto = req.prevLogIndex + 1
        if keep_upto <= self.node.log[-1].index:
            await self.node.logstore.truncate_from(keep_upto)
            self.node.log = self.node.log[:keep_upto]
        # append new
        if req.entries:
            await self.node.logstore.append(req.entries)
            self.node.log.extend(req.entries)

        if req.leaderCommit > self.node.commitIndex:
            self.node.commitIndex = min(req.leaderCommit, self.node.log[-1].index)
        return AEResponse(
            term=self.node.currentTerm,
            success=True,
            nextIndexHint=self.node.log[-1].index + 1,
        )
