package com.breadme.db.server.dm.cache;

import com.breadme.db.server.dm.page.MockPage;
import com.breadme.db.server.dm.page.Page;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MockPageCache implements PageCache {
    private final Map<Integer, MockPage> cache = new HashMap<>();
    private final Lock lock = new ReentrantLock();
    private final AtomicInteger pageNumber = new AtomicInteger(0);
    
    @Override
    public int newPage(byte[] initData) {
        lock.lock();
        try {
            int pageNumber = this.pageNumber.incrementAndGet();
            MockPage page = new MockPage(pageNumber, initData);
            cache.put(pageNumber, page);
            return pageNumber;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Page getPage(int pageNumber) throws Exception {
        lock.lock();
        try {
            return cache.get(pageNumber);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void release(Page page) {

    }

    @Override
    public void truncateByPageNumber(int maxPageNumber) {

    }

    @Override
    public int getPageNumber() {
        return pageNumber.get();
    }

    @Override
    public void flushPage(Page page) {

    }
}
