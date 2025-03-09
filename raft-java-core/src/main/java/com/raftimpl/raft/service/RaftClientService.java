package com.raftimpl.raft.service;

import com.raftimpl.raft.proto.RaftProto;

public interface RaftClientService {
    /**
     * Get leader information from raft cluster
     * @param request
     * @return leader
     */
    RaftProto.GetLeaderResponse getLeader(RaftProto.GetLeaderRequest request);

    /**
     * Get all nodes information from raft cluster
     * @param request
     * @return all nodes endpoints and relationship
     */
    RaftProto.GetConfigurationResponse getConfiguration(RaftProto.GetConfigurationRequest request);

    /**
     * Add peer into raft cluster
     * @param request
     * @return result code
     */
    RaftProto.AddPeersResponse addPeers(RaftProto.AddPeersRequest request);

    /**
     * Delete peer from raft cluster
     * @param request
     * @return result code
     */
    RaftProto.RemovePeersResponse removePeers(RaftProto.RemovePeersRequest request);
}
