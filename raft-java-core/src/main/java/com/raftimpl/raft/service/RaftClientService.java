package com.raftimpl.raft.service;

import com.raftimpl.raft.proto.RaftProto;

public interface RaftClientService {
    /**
     * Get leader information from raft cluster
     * @param request
     * @return leader
     */
    RaftProto.GetLeaderResponse getNowLeader(RaftProto.GetLeaderRequest request);

    /**
     * Get all nodes information from raft cluster
     * @param request
     * @return all nodes endpoints and relationship
     */
    RaftProto.GetConfigurationResponse getConfig(RaftProto.GetConfigurationRequest request);

    /**
     * Add peer into raft cluster
     * @param request
     * @return result code
     */
    RaftProto.AddPeersResponse addStoragePeers(RaftProto.AddPeersRequest request);

    /**
     * Delete peer from raft cluster
     * @param request
     * @return result code
     */
    RaftProto.RemovePeersResponse removeStoragePeers(RaftProto.RemovePeersRequest request);
}
