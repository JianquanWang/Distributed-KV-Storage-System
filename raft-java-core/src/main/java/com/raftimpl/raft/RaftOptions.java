package com.raftimpl.raft;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class RaftOptions {
    // A follower would become a candidate if it doesn't receive any message
    // from the leader in electionTimeoutMs milliseconds
    private int electionTimeoutMilliseconds = 5000;

    // A leader sends RPCs at least this often, even if there is no data to send
    private int heartbeatPeriodMilliseconds = 500;

    // snapshot timer every this often
    private int snapshotPeriodSeconds = 3600;
    // when log entry size reaches to snapshotMinLogSize， it takes a snapshot
    private int snapshotMinLogSize = 100 * 1024 * 1024;
    private int maxSnapshotBytesPerRequest = 500 * 1024; // 500k

    private int maxLogEntriesPerRequest = 5000;

    // single segment file size, default 100 m
    private int maxSegmentFileSize = 100 * 1000 * 1000;

    // follower and leader's last log index below catchupMargin, then they can take participate in voting and serving
    private long catchupMargin = 500;

    // replicate max await time in ms
    private long maxAwaitTimeout = 1000;

    // thread pool size for consensus
    private int raftConsensusThreadNum = 20;

    // enable async write or not; if true, leader node save log and return immediately, then write to followers async-ly
    // if false, log replicating occurs to majority followers then return
    private boolean asyncWrite = false;

    // raft log's and snapshot's parent directory
    private String dataDir = System.getProperty("com.raftimpl.raft.data.dir");
}
