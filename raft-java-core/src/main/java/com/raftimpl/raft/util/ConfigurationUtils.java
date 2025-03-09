package com.raftimpl.raft.util;

import com.raftimpl.raft.proto.RaftProto;

import java.util.List;

public class ConfigurationUtils {

    /**
     * Check if the serverId in configuration server list
     * @param configuration
     * @param serverId
     * @return true if contains
     */
    public static boolean containsServer(RaftProto.Configuration configuration, int serverId) {
        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() == serverId) {
                return true;
            }
        }
        return false;
    }

    public static RaftProto.Configuration removeServers(
            RaftProto.Configuration configuration, List<RaftProto.Server> servers) {
        RaftProto.Configuration.Builder confBuilder = RaftProto.Configuration.newBuilder();
        for (RaftProto.Server server : configuration.getServersList()) {
            boolean toBeRemoved = false;
            for (RaftProto.Server server1 : servers) {
                if (server.getServerId() == server1.getServerId()) {
                    toBeRemoved = true;
                    break;
                }
            }
            if (!toBeRemoved) {
                confBuilder.addServers(server);
            }
        }
        return confBuilder.build();
    }

    public static RaftProto.Server getServer(RaftProto.Configuration configuration, int serverId) {
        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() == serverId) {
                return server;
            }
        }
        return null;
    }
}
