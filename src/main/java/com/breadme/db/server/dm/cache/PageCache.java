package com.breadme.db.server.dm.cache;

import com.breadme.db.server.dm.page.Page;
import com.breadme.db.server.util.FileUtils;
import com.breadme.db.server.util.Panic;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {
    int newPage(byte[] initData);
    Page getPage(int pageNumber) throws Exception;
    void close();
    void release(Page page);
    void truncateByPageNumber(int maxPageNumber);
    int getPageNumber();
    void flushPage(Page page);
    
    static PageCacheImpl create(String dir, String filename, long memory) {
        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(FileUtils.create(dir, filename + PageCacheImpl.FILE_SUFFIX), "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        
        return new PageCacheImpl(raf, fc, (int) memory / Page.PAGE_SIZE);
    }
    
    static PageCacheImpl open(String dir, String filename, long memory) {
        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(FileUtils.open(dir, filename + PageCacheImpl.FILE_SUFFIX), "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        
        return new PageCacheImpl(raf, fc, (int) memory / Page.PAGE_SIZE);
    }
}
