package com.raftimpl.raft.example.server;

import com.baidu.brpc.server.RpcServer;
import com.baidu.brpc.server.RpcServerOptions;
import com.raftimpl.raft.RaftNode;
import com.raftimpl.raft.RaftOptions;
import com.raftimpl.raft.StateMachine;
import com.raftimpl.raft.example.server.machine.LevelDBStateMachine;
import com.raftimpl.raft.example.server.service.ExampleService;
import com.raftimpl.raft.example.server.service.impl.ExampleServiceImpl;
import com.raftimpl.raft.proto.RaftProto;
import com.raftimpl.raft.service.RaftClientService;
import com.raftimpl.raft.service.RaftConsensusService;
import com.raftimpl.raft.service.impl.RaftClientServiceImpl;
import com.raftimpl.raft.service.impl.RaftConsensusServiceImpl;

import java.util.ArrayList;
import java.util.List;

public class ServerMain {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.printf("Usage: ./run_server.sh DATA_PATH CLUSTER CURRENT_NODE\n");
            System.exit(-1);
        }
        // parse args
        // raft data dir
        System.out.print(args);
        String dataPath = args[0];
        // peers, format is "host:port:serverId,host2:port2:serverId2"
        String servers = args[1];
        String[] splitArray = servers.split(",");
        List<RaftProto.Server> serverList = new ArrayList<>();
        for (String serverString : splitArray) {
            RaftProto.Server server = parseServer(serverString);
            serverList.add(server);
        }
        // local server
        RaftProto.Server localServer = parseServer(args[2]);

        // 初始化RPCServer
        RpcServerOptions options = new RpcServerOptions();
        options.setIoThreadNum(Runtime.getRuntime().availableProcessors() * 10);
        options.setWorkThreadNum(Runtime.getRuntime().availableProcessors() * 10);
        RpcServer server = new RpcServer(localServer.getEndpoint().getPort(), options);
        // 设置Raft选项，比如：
        // just for test snapshot
        RaftOptions RaftOptions = new RaftOptions();
        RaftOptions.setDataDir(dataPath);
        RaftOptions.setSnapshotMinLogSize(10 * 1024);
        RaftOptions.setSnapshotPeriodSeconds(30);
        RaftOptions.setMaxSegmentFileSize(1024 * 1024);
        // 应用状态机
        StateMachine stateMachine =
                //    new HashMapStateMachine(RaftOptions.getDataDir());
                new LevelDBStateMachine(RaftOptions.getDataDir());
//                new BTreeStateMachine(RaftOptions.getDataDir());
//                new BitCaskStateMachine(RaftOptions.getDataDir());
        // 初始化RaftNode
        RaftNode raftNode = new RaftNode(RaftOptions, serverList, localServer, stateMachine);
        // 注册Raft节点之间相互调用的服务
        RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);
        server.registerService(raftConsensusService);
        // 注册给Client调用的Raft服务
        RaftClientService raftClientService = new RaftClientServiceImpl(raftNode);
        server.registerService(raftClientService);
        // 注册应用自己提供的服务
        ExampleService exampleService = new ExampleServiceImpl(raftNode, stateMachine);
        server.registerService(exampleService);
        // 启动RPCServer，初始化Raft节点
        server.start();
        raftNode.init();
    }

    private static RaftProto.Server parseServer(String serverString) {
        String[] splitServer = serverString.split(":");
        String host = splitServer[0];
        Integer port = Integer.parseInt(splitServer[1]);
        Integer serverId = Integer.parseInt(splitServer[2]);
        RaftProto.Endpoint endPoint = RaftProto.Endpoint.newBuilder()
                .setHost(host).setPort(port).build();
        RaftProto.Server.Builder serverBuilder = RaftProto.Server.newBuilder();
        RaftProto.Server server = serverBuilder.setServerId(serverId).setEndpoint(endPoint).build();
        return server;
    }
}
