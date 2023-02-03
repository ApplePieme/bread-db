package com.breadme.db.server.dm.page;

public interface Page {
    int PAGE_SIZE = 1 << 13;
    
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
