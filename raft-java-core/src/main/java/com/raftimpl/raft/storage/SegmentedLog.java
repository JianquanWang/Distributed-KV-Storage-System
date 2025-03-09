package com.raftimpl.raft.storage;

import com.raftimpl.raft.proto.RaftProto;
import com.raftimpl.raft.util.RaftFIleUtils;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class SegmentedLog {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentedLog.class);

    private final String logDir;
    private final String logDataDir;
    private final int maxSegmentFileSize;
    @Getter
    private RaftProto.LogMetaData metaData;
    private final TreeMap<Long, Segment> startLogIndexSegmentMap = new TreeMap<>();
    private volatile AtomicLong totalSize;

    public SegmentedLog(String raftDataDir, int maxSegmentFileSize) {
        this.logDir = raftDataDir + File.separator + "logs";
        this.logDataDir = logDir + File.separator + "data";
        this.maxSegmentFileSize = maxSegmentFileSize;
        File file = new File(logDataDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        readSegments();
        for (Segment segment : startLogIndexSegmentMap.values()) {
            this.loadSegmentData(segment);
        }
        // get current_term, voted_for, first_log_index, commit_index from metadata file
        metaData = this.readMetaData();
        if (metaData == null) {
            if (!startLogIndexSegmentMap.isEmpty()) {
                LOG.error("No readable metadata file but found segments in {}", logDir);
                throw new RuntimeException("No readable metadata file but found segments");
            }
            metaData = RaftProto.LogMetaData.newBuilder().setFirstLogIndex(1).build();
        }
    }

    /**
     * read segments from folder, segments are named as "open-" "startIndex-endIndex"
     * and put to map in k-V form as "start index" : segment
     */
    public void readSegments(){
        try {
            List<String> fileNames = RaftFIleUtils.getSortedFilesInDirectory(logDataDir, logDataDir);
            for (String fileName : fileNames) {
                String[] splitArray = fileName.split("-");
                if (splitArray.length != 2) {
                    LOG.warn("Invalid segment file name: {}", fileName);
                    continue;
                }
                Segment segment = new Segment();
                segment.setFileName(fileName);
                if (splitArray[0].equals("open")) {
                    segment.setCanWrite(true);
                    segment.setStartIndex(Long.parseLong(splitArray[1]));
                    segment.setEndIndex(0);
                } else{
                    try{
                    segment.setCanWrite(false);
                    segment.setStartIndex(Long.parseLong(splitArray[0]));
                    segment.setEndIndex(Long.parseLong(splitArray[1]));
                    } catch (NumberFormatException e) {
                        LOG.warn("Invalid segment file name: {}", fileName);
                        continue;
                    }
                }

                segment.setRandomAccessFile(RaftFIleUtils.openFile(logDataDir, fileName, "rw"));
                segment.setFileSize(segment.getRandomAccessFile().length());
                startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
            }
        } catch (IOException e) {
            LOG.warn("readSegments exception:", e);
            throw new RuntimeException("open segment file error");
        }
    }

    /**
     * set up entries list and reset startIndex and endIndex for segment
     * @param segment
     */
    public void loadSegmentData(Segment segment) {
        RandomAccessFile randomAccessFile = segment.getRandomAccessFile();
        long totalLength = segment.getFileSize();
        long offset = 0;
        while (offset < totalLength) {
            // read log entry from segment, which is the message part in segment file
            RaftProto.LogEntry entry = RaftFIleUtils.readProtoFromFile(randomAccessFile, RaftProto.LogEntry.class);
            if (entry == null) {
                throw new RuntimeException("read segment log failed");
            }
            // put record to segment object
            segment.getEntries().add(new Segment.Record(offset, entry));
            try {
                // update offset
                offset = randomAccessFile.getFilePointer();
            } catch (IOException e) {
                LOG.error("read segment meet exception, msg={}", e.getMessage());
                throw new RuntimeException("file not found");
            }
        }
        int entrySize = segment.getEntries().size();
        if (entrySize > 0) {
            segment.setStartIndex(segment.getEntries().get(0).entry.getIndex());
            segment.setEndIndex(segment.getEntries().get(entrySize - 1).entry.getIndex());
        }
    }

    /**
     * get current_term, voted_for, first_log_index, commit_index from metadata file
     * @return metadata
     */
    public RaftProto.LogMetaData readMetaData() {
        String fileName = logDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            return RaftFIleUtils.readProtoFromFile(randomAccessFile, RaftProto.LogMetaData.class);
        }catch (IOException e) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }

    /**
     * Get entry from tree map startLogIndexSegmentMap
     * @param index
     * @return entry
     */
    public RaftProto.LogEntry getEntry(long index) {
        long firstLogIndex = getFirstLogIndex();
        long lastLogIndex = getLastLogIndex();
        if (index == 0 || index < firstLogIndex || index > lastLogIndex) {
            LOG.debug("index out of range, index={}, firstLogIndex={}, lastLogIndex={}",
                    index, firstLogIndex, lastLogIndex);
            return null;
        }
        if (startLogIndexSegmentMap.isEmpty()) {
            return null;
        }
        Segment segment = startLogIndexSegmentMap.floorEntry(index).getValue();
        return segment.getEntry(index);
    }

    /**
     * Get given index's entry term
     * @param index
     * @return term
     */
    public long getEntryTerm(long index) {
        RaftProto.LogEntry entry = getEntry(index);
        if (entry == null) {
            return 0;
        }
        return entry.getTerm();
    }

    /**
     * Get entry's term
     * @param entry
     * @return term
     */
    public long getEntryTerm(RaftProto.LogEntry entry) {
        if (entry == null) {
            return 0;
        }
        return entry.getTerm();
    }

    public long getFirstLogIndex() {
        return metaData.getFirstLogIndex();
    }

    /**
     * Get last log index. There are two situations the segment is empty:
     * 1. The first time init, firstLogIndex = 1, lastLogIndex = 0
     * 2. snapshot is just finished, the log is cleaned, firstLogIndex = snapshotIndex + 1, lastLogIndex = snapshotIndex
     * @return
     */
    public long getLastLogIndex() {
        if (startLogIndexSegmentMap.isEmpty()) {
            return getFirstLogIndex() - 1;
        }
        Segment lastSegment = startLogIndexSegmentMap.lastEntry().getValue();
        return lastSegment.getEndIndex();
    }

    /**
     * Append entries to segment logs
     * @param entries
     * @return the last log index
     */
    public long append(List<RaftProto.LogEntry> entries) {
        long newLastLogIndex = this.getLastLogIndex();
        for (RaftProto.LogEntry entry : entries) {
            newLastLogIndex++;
            int entrySize = entry.getSerializedSize();
            // 3 situations to create new segment file:
            // 1. No segment file exists
            // 2. The last segment file is not writable
            // 3. The last segment file size plus entry size is over the limit of segment file size
            boolean isNeedNewSegmentFile = false;
            try {
                if (startLogIndexSegmentMap.isEmpty()) {
                    isNeedNewSegmentFile = true;
                } else {
                    // Get the last segment from map
                    Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
                    if (!segment.isCanWrite()) {
                        isNeedNewSegmentFile = true;
                    } else if (segment.getFileSize() + entrySize >= maxSegmentFileSize) {
                        isNeedNewSegmentFile = true;
                        // Change the segment's name and set it close
                        segment.getRandomAccessFile().close();
                        segment.setCanWrite(false);
                        String newFileName = String.format("%020d-%020d",
                                segment.getStartIndex(), segment.getEndIndex());
                        String newFullFileName = logDataDir + File.separator + newFileName;
                        File newFile = new File(newFullFileName);
                        String oldFullFileName = logDataDir + File.separator + segment.getFileName();
                        File oldFile = new File(oldFullFileName);
                        FileUtils.moveFile(oldFile, newFile);
                        segment.setFileName(newFileName);
                        segment.setRandomAccessFile(RaftFIleUtils.openFile(logDataDir, newFileName, "r"));
                    }
                }
                // create new segment file
                Segment newSegment;
                if (isNeedNewSegmentFile) {
                    String newSegmentFileName = String.format("open-%d", newLastLogIndex);
                    String newFullFileName = logDataDir + File.separator + newSegmentFileName;
                    File newSegmentFile = new File(newFullFileName);
                    if (!newSegmentFile.exists()) {
                        newSegmentFile.createNewFile();
                    }
                    Segment segment = new Segment();
                    segment.setCanWrite(true);
                    segment.setStartIndex(newLastLogIndex);
                    segment.setEndIndex(0);
                    segment.setFileName(newSegmentFileName);
                    segment.setRandomAccessFile(RaftFIleUtils.openFile(logDataDir, newSegmentFileName, "rw"));
                    newSegment = segment;
                } else {
                    newSegment = startLogIndexSegmentMap.lastEntry().getValue();
                }
                // write proto to segment
                if (entry.getIndex() == 0) {
                    entry = RaftProto.LogEntry.newBuilder(entry)
                            .setIndex(newLastLogIndex).build();
                }
                // set segment end index as the entry's latest index
                newSegment.setEndIndex(entry.getIndex());
                // add new record with current file offset and entry
                newSegment.getEntries().add(new Segment.Record(
                        newSegment.getRandomAccessFile().getFilePointer(), entry));
                // write entry to segment file
                RaftFIleUtils.writeProtoToFile(newSegment.getRandomAccessFile(), entry);
                newSegment.setFileSize(newSegment.getRandomAccessFile().length());
                if (!startLogIndexSegmentMap.containsKey(newSegment.getStartIndex())) {
                    startLogIndexSegmentMap.put(newSegment.getStartIndex(), newSegment);
                }
                totalSize.getAndAdd(entrySize);
            }  catch (IOException ex) {
                throw new RuntimeException("append raft log exception, msg=" + ex.getMessage());
            }
        }
        return newLastLogIndex;
    }

    public void truncatePrefix(long newFirstIndex) {
        if (newFirstIndex <= getFirstLogIndex()) {
            return;
        }
        long oldFirstIndex = getFirstLogIndex();
        while (!startLogIndexSegmentMap.isEmpty()) {
            Segment segment = startLogIndexSegmentMap.firstEntry().getValue();
            if (segment.isCanWrite()) {
                break;
            }
            if (newFirstIndex > segment.getEndIndex()) {
                File oldFile = new File(logDataDir + File.separator + segment.getFileName());
                try {
                    RaftFIleUtils.closeFile(segment.getRandomAccessFile());
                    FileUtils.forceDelete(oldFile);
                    totalSize.getAndAdd(-segment.getFileSize());
                    startLogIndexSegmentMap.remove(segment.getStartIndex());
                } catch (Exception ex2) {
                    LOG.warn("delete file exception:", ex2);
                }
            } else {
                break;
            }
        }
        long newActualFirstIndex;
        if (startLogIndexSegmentMap.isEmpty()) {
            newActualFirstIndex = newFirstIndex;
        } else {
            newActualFirstIndex = startLogIndexSegmentMap.firstKey();
        }
        updateMetaData(null, null, newActualFirstIndex, null);
        LOG.info("Truncating log from old first index {} to new first index {}",
                oldFirstIndex, newActualFirstIndex);
    }

    public void truncateSuffix(long newEndIndex) {
        if (newEndIndex >= getLastLogIndex()) {
            return;
        }
        LOG.info("Truncating log from old end index {} to new end index {}",
                getLastLogIndex(), newEndIndex);
        while (!startLogIndexSegmentMap.isEmpty()) {
            Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
            try {
                if (newEndIndex == segment.getEndIndex()) {
                    break;
                } else if (newEndIndex < segment.getStartIndex()) {
                    totalSize.getAndAdd(-segment.getFileSize());
                    // delete file
                    segment.getRandomAccessFile().close();
                    String fullFileName = logDataDir + File.separator + segment.getFileName();
                    FileUtils.forceDelete(new File(fullFileName));
                    startLogIndexSegmentMap.remove(segment.getStartIndex());
                } else if (newEndIndex < segment.getEndIndex()) {
                    int i = (int) (newEndIndex + 1 - segment.getStartIndex());
                    segment.setEndIndex(newEndIndex);
                    long newFileSize = segment.getEntries().get(i).offset;
                    totalSize.getAndAdd(-(segment.getFileSize() - newFileSize));
                    segment.setFileSize(newFileSize);
                    segment.getEntries().removeAll(
                            segment.getEntries().subList(i, segment.getEntries().size()));
                    FileChannel fileChannel = segment.getRandomAccessFile().getChannel();
                    fileChannel.truncate(segment.getFileSize());
                    fileChannel.close();
                    segment.getRandomAccessFile().close();
                    String oldFullFileName = logDataDir + File.separator + segment.getFileName();
                    String newFileName = String.format("%020d-%020d",
                            segment.getStartIndex(), segment.getEndIndex());
                    segment.setFileName(newFileName);
                    String newFullFileName = logDataDir + File.separator + segment.getFileName();
                    new File(oldFullFileName).renameTo(new File(newFullFileName));
                    segment.setRandomAccessFile(RaftFIleUtils.openFile(logDataDir, segment.getFileName(), "rw"));
                }
            } catch (IOException ex) {
                LOG.warn("io exception, msg={}", ex.getMessage());
            }
        }
    }

    /**
     * upate current term, voted for, first log index and commit index to metadata
     * @param currentTerm
     * @param votedFor
     * @param firstLogIndex
     * @param commitIndex
     */
    public void updateMetaData(Long currentTerm, Integer votedFor, Long firstLogIndex, Long commitIndex) {
        RaftProto.LogMetaData.Builder builder = RaftProto.LogMetaData.newBuilder(this.metaData);
        if (currentTerm != null) {
            builder.setCurrentTerm(currentTerm);
        }
        if (votedFor != null) {
            builder.setVotedFor(votedFor);
        }
        if (firstLogIndex != null) {
            builder.setFirstLogIndex(firstLogIndex);
        }
        if (commitIndex != null) {
            builder.setCommitIndex(commitIndex);
        }
        this.metaData = builder.build();

        String fileName = logDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            RaftFIleUtils.writeProtoToFile(randomAccessFile, metaData);
            LOG.info("new segment meta info, currentTerm={}, votedFor={}, firstLogIndex={}",
                    metaData.getCurrentTerm(), metaData.getVotedFor(), metaData.getFirstLogIndex());
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
        }
    }

    public long getTotalSize() {
        return totalSize.get();
    }

}
