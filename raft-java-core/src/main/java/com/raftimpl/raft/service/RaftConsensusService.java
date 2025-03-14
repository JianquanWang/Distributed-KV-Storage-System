package com.raftimpl.raft.service;

import com.raftimpl.raft.proto.RaftProto;

public interface RaftConsensusService {
    RaftProto.VoteResponse preVote(RaftProto.VoteRequest request);
    RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request);
    RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request);
    RaftProto.InstallSnapshotResponse installSnap(RaftProto.InstallSnapshotRequest request);
    RaftProto.GetLeaderCommitIndexResponse getLeaderCommitIndex(RaftProto.GetLeaderCommitIndexRequest request);
}
