package com.raftimpl.raft;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.instance.Endpoint;
import com.raftimpl.raft.proto.RaftProto;
import com.raftimpl.raft.service.RaftConsensusServiceAsync;
import lombok.Getter;
import lombok.Setter;

public class Peer {
    @Getter
    private RaftProto.Server server;
    @Getter
    private RpcClient raftRpcClient;
    @Getter
    private RpcClient applicationRpcClient;
    @Getter
    private final RaftConsensusServiceAsync raftConsensusServiceAsync;
    @Getter
    @Setter
    // the next index will send to followers
    private long nextIndex;
    @Getter
    @Setter
    // the latest index of replicated log
    private long matchIndex;
    @Setter
    private volatile Boolean voteGranted;
    private volatile boolean isCatchUp;


    public Peer(RaftProto.Server server) {
        this.server = server;
        this.raftRpcClient = new RpcClient(new Endpoint(
                server.getEndpoint().getHost(),
                server.getEndpoint().getPort()));
        raftConsensusServiceAsync = BrpcProxy.getProxy(raftRpcClient, RaftConsensusServiceAsync.class);
        isCatchUp = false;
    }

    public RpcClient createClient() {
        return new RpcClient(new Endpoint(
                server.getEndpoint().getHost(),
                server.getEndpoint().getPort()
        ));
    }

    public RaftProto.Server getStorageServer() {
        return server;
    }

    public Boolean isVoteGranted() {
        return voteGranted;
    }


    public boolean isCatchUp() {
        return isCatchUp;
    }

    public void setCatchUp(boolean catchUp) {
        isCatchUp = catchUp;
    }
}
