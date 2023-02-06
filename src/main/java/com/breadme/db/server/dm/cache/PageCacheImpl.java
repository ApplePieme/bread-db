package com.breadme.db.server.dm.cache;

import com.breadme.db.common.Error;
import com.breadme.db.server.common.AbstractCache;
import com.breadme.db.server.dm.page.Page;
import com.breadme.db.server.dm.page.PageImpl;
import com.breadme.db.server.util.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    static final String FILE_SUFFIX = ".db";
    private static final int CAPACITY_MIN_LIMIT = 10;
    
    private final RandomAccessFile raf;
    private final FileChannel fc;
    private final Lock fileLock;
    private final AtomicInteger pageNumber;
    
    PageCacheImpl(RandomAccessFile raf, FileChannel fc, int capacity) {
        super(capacity);
        
        if (capacity < CAPACITY_MIN_LIMIT) {
            Panic.panic(Error.MemoryTooSmallException);
        }
        
        long len = 0;
        try {
            len = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        
        this.raf = raf;
        this.fc = fc;
        fileLock = new ReentrantLock();
        pageNumber = new AtomicInteger((int) len / Page.PAGE_SIZE);
    }
    
    @Override
    protected Page getForCache(long key) throws Exception {
        int pageNumber = (int) key;
        long offset = getPageOffset(pageNumber);
        ByteBuffer buf = ByteBuffer.allocate(Page.PAGE_SIZE);
        
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        
        return new PageImpl(pageNumber, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page obj) {
        if (obj.isDirty()) {
            flush(obj);
            obj.setDirty(false);
        }
    }

    @Override
    public int newPage(byte[] initData) {
        int pageNumber = this.pageNumber.incrementAndGet();
        Page page = new PageImpl(pageNumber, initData, null);
        flush(page);
        return pageNumber;
    }

    @Override
    public Page getPage(int pageNumber) throws Exception {
        return get(pageNumber);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    @Override
    public void truncateByPageNumber(int maxPageNumber) {
        long size = getPageOffset(maxPageNumber + 1);
        try {
            raf.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumber.set(maxPageNumber);
    }

    @Override
    public int getPageNumber() {
        return pageNumber.get();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }
    
    private void flush(Page page) {
        int pageNumber = page.getPageNumber();
        long offset = getPageOffset(pageNumber);
        
        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }
    
    private long getPageOffset(int pageNumber) {
        return (long) (pageNumber - 1) * Page.PAGE_SIZE;
    }
}
