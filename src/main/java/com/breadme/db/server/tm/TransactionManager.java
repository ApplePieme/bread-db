package com.breadme.db.server.tm;

import com.breadme.db.common.Error;
import com.breadme.db.server.util.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManager {
    long begin();
    void commit(long xid);
    void abort(long xid);
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);
    void close();
    
    static TransactionManagerImpl create(String dir, String filename) {
        if (!dir.endsWith("/")) {
            dir += "/";
        }
        File file = new File(dir + filename + TransactionManagerImpl.FILE_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
            ByteBuffer buf = ByteBuffer.allocate(TransactionManagerImpl.LEN_XID_HEADER_LENGTH);
            buf.putLong(0L);
            buf.flip();
            fc.position(0L);
            fc.write(buf);
        } catch (Exception e) {
            Panic.panic(e);
        }
        
        return new TransactionManagerImpl(raf, fc);
    }
    
    static TransactionManagerImpl open(String dir, String filename) {
        if (!dir.endsWith("/")) {
            dir += "/";
        }
        File file = new File(dir + filename + TransactionManagerImpl.FILE_SUFFIX);
        if (!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        
        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        
        return new TransactionManagerImpl(raf, fc);
    }
}
