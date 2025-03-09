package com.raftimpl.raft.util;

import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

public class RaftFileUtils {
    private static final Logger LOG = LoggerFactory.getLogger(RaftFileUtils.class);

    public static List<String> getSortedFilesInDirectory(String dirName, String rootDirName) throws IOException {
        List<String> fileList = new ArrayList<>();
        File rootDir = new File(rootDirName);
        File dir = new File(dirName);
        if (!rootDir.isDirectory() || !dir.isDirectory()){
            return fileList;
        }
        String rootPath = rootDir.getCanonicalPath();
        if (!rootPath.endsWith("/")){
            rootPath = rootPath + "/";
        }
        File[] files = dir.listFiles();
        for(File file : files){
            if (file.isDirectory()){
                fileList.addAll(getSortedFilesInDirectory(file.getCanonicalPath(), rootPath));
            } else {
                fileList.add(file.getCanonicalPath().substring(rootPath.length()));
            }
        }
        Collections.sort(fileList);
        return fileList;
    }

    public static RandomAccessFile openFile(String dir, String fileName, String mode){
        String fullFileName = dir + File.separator + fileName;
        File file = new File(fullFileName);
        try {
            return new RandomAccessFile(file, mode);
        } catch (FileNotFoundException e) {
            LOG.warn("file not fount, file={}", fileName);
            throw new RuntimeException("file not found, file=" + fileName);
        }
    }

    public static void closeFile(RandomAccessFile file){
        if (file != null){
            try {
                file.close();
            } catch (IOException e) {
                LOG.warn("close file error, msg={}", e.getMessage());
            }
        }
    }

    public static void closeFile(FileInputStream fis){
        if (fis != null){
            try {
                fis.close();
            } catch (IOException e) {
                LOG.warn("close file error, msg={}", e.getMessage());
            }
        }
    }
    public static void closeFile(FileOutputStream fos){
        if (fos != null){
            try {
                fos.close();
            } catch (IOException e) {
                LOG.warn("close file error, msg={}", e.getMessage());
            }
        }
    }

    public static <T extends Message> T readProtoFromFile(RandomAccessFile raf, Class<T> clazz) {
        try {
            long crc32FromFile = raf.readLong();
            int dataLen = raf.readInt();
            int hasReadLen = (Long.SIZE + Integer.SIZE) / Byte.SIZE;
            if (raf.length() - hasReadLen < dataLen) {
                LOG.warn("file remainLength < dataLen");
                return null;
            }
            byte[] data = new byte[dataLen];
            int readLen = raf.read(data);
            if (readLen != dataLen) {
                LOG.warn("readLen != dataLen");
                return null;
            }
            long crc32FromData = getCRC32(data);
            if (crc32FromFile != crc32FromData) {
                LOG.warn("crc32 check failed");
                return null;
            }
            Method method = clazz.getMethod("parseFrom", byte[].class);
            T message = (T) method.invoke(clazz, data);
            return message;
        } catch (Exception ex) {
            LOG.warn("readProtoFromFile meet exception, {}", ex.getMessage());
            return null;
        }
    }

    public static  <T extends Message> void writeProtoToFile(RandomAccessFile raf, T message) {
        byte[] messageBytes = message.toByteArray();
        long crc32 = getCRC32(messageBytes);
        try {
            raf.writeLong(crc32);
            raf.writeInt(messageBytes.length);
            raf.write(messageBytes);
        } catch (IOException ex) {
            LOG.warn("write proto to file error, msg={}", ex.getMessage());
            throw new RuntimeException("write proto to file error");
        }
    }

    public static long getCRC32(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return crc32.getValue();
    }

}
