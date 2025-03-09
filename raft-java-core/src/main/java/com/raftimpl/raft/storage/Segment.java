package com.raftimpl.raft.storage;

import com.raftimpl.raft.proto.RaftProto;
import lombok.Getter;
import lombok.Setter;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
public class Segment {
    public static class Record {
        public long offset;
        public RaftProto.LogEntry entry; // contains term, index, entry_type and data
        public Record(long offset, RaftProto.LogEntry entry) {
            this.offset = offset;
            this.entry = entry;
        }
    }

    private boolean canWrite;
    private long startIndex;
    private long endIndex;
    private long fileSize;
    private String fileName;
    private RandomAccessFile randomAccessFile;
    private List<Record> entries = new ArrayList<>();

    public RaftProto.LogEntry getEntry(long index) {
        if (startIndex == 0  || endIndex == 0) {
            return null;
        }
        if (index < startIndex || index > endIndex) {
            return null;
        }
        int indexInList = (int) (index - startIndex);
        return entries.get(indexInList).entry;
    }

}
