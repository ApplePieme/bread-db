package com.breadme.db.server.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MockPage implements Page {
    private final int pageNumber;
    private final byte[] data;
    private final Lock lock;
    
    public MockPage(int pageNumber, byte[] data) {
        this.pageNumber = pageNumber;
        this.data = data;
        lock = new ReentrantLock();
    }
    
    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {

    }

    @Override
    public void setDirty(boolean dirty) {

    }

    @Override
    public boolean isDirty() {
        return false;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
