package com.raftimpl.raft.service;

import com.raftimpl.raft.proto.RaftProto;
import java.util.concurrent.Future;
import com.baidu.brpc.client.RpcCallback;

public interface RaftConsensusServiceAsync extends RaftConsensusService {
    Future<RaftProto.VoteResponse> preVote(
            RaftProto.VoteRequest request,
            RpcCallback<RaftProto.VoteResponse> callback);
    Future<RaftProto.VoteResponse> requestVote(
            RaftProto.VoteRequest request,
            RpcCallback<RaftProto.VoteResponse> callback);
    Future<RaftProto.AppendEntriesResponse> appendEntries(
            RaftProto.AppendEntriesRequest request,
            RpcCallback<RaftProto.AppendEntriesResponse> callback);
    Future<RaftProto.InstallSnapshotResponse> installSnapshot(
            RaftProto.InstallSnapshotRequest request,
            RpcCallback<RaftProto.InstallSnapshotResponse> callback);
}
