package com.breadme.db.server.common;

public class MockCache extends AbstractCache<Long> {
    public MockCache(int capacity) {
        super(capacity);
    }
    
    @Override
    protected Long getForCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseForCache(Long obj) {

    }
}
