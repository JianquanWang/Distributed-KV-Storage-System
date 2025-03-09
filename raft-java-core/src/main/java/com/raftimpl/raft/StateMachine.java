package com.raftimpl.raft;

public interface StateMachine {

    void writeSnapshot(String snapshotDir);

    void readSnapshot(String snapshotDir);

    void apply(byte[] dataBytes);
}
