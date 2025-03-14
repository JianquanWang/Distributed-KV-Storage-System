package com.raftimpl.raft.service;

import com.raftimpl.raft.proto.RaftProto;
import java.util.concurrent.Future;
import com.baidu.brpc.client.RpcCallback;

public interface RaftClientServiceAsync extends RaftClientService {
    Future<RaftProto.GetLeaderResponse> getNowLeader(
            RaftProto.GetLeaderRequest request,
            RpcCallback<RaftProto.GetLeaderResponse> callback);

    Future<RaftProto.GetConfigurationResponse> getConfig(
            RaftProto.GetConfigurationRequest request,
            RpcCallback<RaftProto.GetConfigurationResponse> callback);

    Future<RaftProto.AddPeersResponse> addStoragePeers(
            RaftProto.AddPeersRequest request,
            RpcCallback<RaftProto.AddPeersResponse> callback);

    Future<RaftProto.RemovePeersResponse> removeStoragePeers(
            RaftProto.RemovePeersRequest request,
            RpcCallback<RaftProto.RemovePeersResponse> callback);
}
