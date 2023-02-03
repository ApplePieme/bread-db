package com.breadme.db.server.dm.cache;

import com.breadme.db.common.Error;
import com.breadme.db.server.dm.page.Page;
import com.breadme.db.server.util.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
    
    static PageCacheImpl create(String path, long memory) {
        File file = new File(path);
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
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        
        return new PageCacheImpl(raf, fc, (int) memory / Page.PAGE_SIZE);
    }
    
    static PageCacheImpl open(String path, long memory) {
        File file = new File(path);
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
        
        return new PageCacheImpl(raf, fc, (int) memory / Page.PAGE_SIZE);
    }
}
