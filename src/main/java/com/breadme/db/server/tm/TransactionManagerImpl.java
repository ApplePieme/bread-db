package com.breadme.db.server.tm;

import com.breadme.db.common.Error;
import com.breadme.db.server.util.Panic;
import com.breadme.db.server.util.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager {
    static final int LEN_XID_HEADER_LENGTH = 8;
    private static final int XID_FIELD_SIZE = 1;
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;
    private static final long SUPER_XID = 0;
    
    private final RandomAccessFile raf;
    private final FileChannel fc;
    private final Lock counterLock;
    private long xidCounter;
    
    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXidCounter();
    }
    
    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXid(xid, FIELD_TRAN_ACTIVE);
            incrXidCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    @Override
    public void commit(long xid) {
        updateXid(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXid(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXid(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) {
            return true;
        }
        return checkXid(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXid(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
    
    private boolean checkXid(long xid, byte status) {
        ByteBuffer buf = ByteBuffer.allocate(XID_FIELD_SIZE);
        try {
            fc.position(getXidPosition(xid));
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }
    
    private void checkXidCounter() {
        long fileLen = 0;
        try {
            fileLen = raf.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXidFileException);
        }
        
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXidFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0L);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        
        xidCounter = Parser.parseLong(buf.array());
        if (getXidPosition(xidCounter + 1) != fileLen) {
            Panic.panic(Error.BadXidFileException);
        }
    }
    
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }
    
    private void updateXid(long xid, byte status) {
        ByteBuffer buf = ByteBuffer.allocate(XID_FIELD_SIZE);
        buf.put(status);
        buf.flip();
        try {
            fc.position(getXidPosition(xid));
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
    
    private void incrXidCounter() {
        ++xidCounter;
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        buf.putLong(xidCounter);
        buf.flip();
        try {
            fc.position(0L);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
