package com.raftimpl.raft.admin;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.instance.Endpoint;
import com.raftimpl.raft.proto.RaftProto;
import com.raftimpl.raft.service.RaftClientService;
import com.googlecode.protobuf.format.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RaftClientServiceProxy implements RaftClientService {
    private static final Logger LOG = LoggerFactory.getLogger(RaftClientServiceProxy.class);
    private static final JsonFormat jsonFormat = new JsonFormat();

    private List<RaftProto.Server> cluster;
    private RpcClient clusterRPCClient;
    private RaftClientService clusterRaftClientService;

    private RaftProto.Server leader;
    private RpcClient leaderRPCClient;
    private RaftClientService leaderRaftClientService;

    private RpcClientOptions rpcClientOptions = new RpcClientOptions();

    // servers format is 10.1.1.1:8888,10.2.2.2:9999
    public RaftClientServiceProxy(String ipPorts) {
        rpcClientOptions.setConnectTimeoutMillis(1000); // 1s
        rpcClientOptions.setReadTimeoutMillis(3600000); // 1hour
        rpcClientOptions.setWriteTimeoutMillis(1000); // 1s
        clusterRPCClient = new RpcClient(ipPorts, rpcClientOptions);
        clusterRaftClientService = BrpcProxy.getProxy(clusterRPCClient, RaftClientService.class);
        updateConfig();
    }

    @Override
    public RaftProto.GetLeaderResponse getNowLeader(RaftProto.GetLeaderRequest request) {
        return clusterRaftClientService.getNowLeader(request);
    }

    @Override
    public RaftProto.GetConfigurationResponse getConfig(RaftProto.GetConfigurationRequest request) {
        return clusterRaftClientService.getConfig(request);
    }

    @Override
    public RaftProto.AddPeersResponse addStoragePeers(RaftProto.AddPeersRequest request) {
        RaftProto.AddPeersResponse response = leaderRaftClientService.addStoragePeers(request);
        if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_NOT_LEADER) {
            updateConfig();
            response = leaderRaftClientService.addStoragePeers(request);
        }
        return response;
    }

    @Override
    public RaftProto.RemovePeersResponse removeStoragePeers(RaftProto.RemovePeersRequest request) {
        RaftProto.RemovePeersResponse response = leaderRaftClientService.removeStoragePeers(request);
        if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_NOT_LEADER) {
            updateConfig();
            response = leaderRaftClientService.removeStoragePeers(request);
        }
        return response;
    }

    public void stop() {
        if (leaderRPCClient != null) {
            leaderRPCClient.stop();
        }
        if (clusterRPCClient != null) {
            clusterRPCClient.stop();
        }
    }

    private boolean updateConfig() {
        RaftProto.GetConfigurationRequest request = RaftProto.GetConfigurationRequest.newBuilder().build();
        RaftProto.GetConfigurationResponse response = clusterRaftClientService.getConfig(request);
        if (response != null && response.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
            if (leaderRPCClient != null) {
                leaderRPCClient.stop();
            }
            leader = response.getLeader();
            leaderRPCClient = new RpcClient(convertEndPoint(leader.getEndpoint()), rpcClientOptions);
            leaderRaftClientService = BrpcProxy.getProxy(leaderRPCClient, RaftClientService.class);
            return true;
        }
        return false;
    }

    private Endpoint convertEndPoint(RaftProto.Endpoint endPoint) {
        return new Endpoint(endPoint.getHost(), endPoint.getPort());
    }

}
