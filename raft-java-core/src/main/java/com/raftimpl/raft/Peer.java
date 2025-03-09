package com.raftimpl.raft;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.Endpoint;
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
    @Setter
    @Getter
    // the next index will send to followers
    private long nextIndex;
    @Setter
    @Getter
    // the latest index of replicated log
    private long matchIndex;
    @Setter
    private volatile Boolean voteGranted;
    private volatile boolean isCatchUp;

    public Peer(RaftProto.Server server) {
        this.server = server;
        this.raftRpcClient = new RpcClient(new Endpoint(server.getEndpoint().getHost(), server.getEndpoint().getPort()));
        raftConsensusServiceAsync = BrpcProxy.getProxy(raftRpcClient, RaftConsensusServiceAsync.class);
        isCatchUp = false;
    }

    public void setApplicationRpcClient(){
        this.applicationRpcClient = new RpcClient(new Endpoint(server.getEndpoint().getHost(), server.getEndpoint().getPort()));
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
