package com.breadme.db.server.dm.cache;

import com.breadme.db.server.dm.page.Page;
import com.breadme.db.server.util.Panic;
import com.breadme.db.server.util.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheTest {
    private static final Random random = new SecureRandom();
    
    @Test
    public void pageCacheTest() throws Exception {
        PageCache pageCache = PageCache.create("./", "page-cache-simple-test0", Page.PAGE_SIZE * 50);
        
        for (int i = 0; i < 100; i++) {
            byte[] data = new byte[Page.PAGE_SIZE];
            data[0] = (byte) i;
            int pageNum = pageCache.newPage(data);
            Page page = pageCache.getPage(pageNum);
            page.setDirty(true);
            page.release();
        }
        pageCache.close();
        
        pageCache = PageCache.open("./", "page-cache-simple-test0", Page.PAGE_SIZE * 50);
        for (int i = 1; i <= 100; i++) {
            Page page = pageCache.getPage(i);
            Assert.assertEquals(page.getData()[0], (byte) i - 1);
            page.release();
        }
        pageCache.close();
        
        Assert.assertTrue(new File("page-cache-simple-test0.db").delete());
    }
    
    @Test
    public void simpleMultiPageCacheTest() {
        PageCache pageCache = PageCache.create("./", "page-cache-simple-test1", Page.PAGE_SIZE * 100);
        AtomicInteger pageNumGen = new AtomicInteger(0);

        for (int i = 0; i < 200; i++) {
            for (int j = 0; j < 80; j++) {
                int op = Math.abs(random.nextInt() % 20);
                if (op == 0) {
                    byte[] data = RandomUtils.randomBytes(Page.PAGE_SIZE);
                    int pageNum = pageCache.newPage(data);
                    try {
                        Page page = pageCache.getPage(pageNum);
                        pageNumGen.incrementAndGet();
                        page.release();
                    } catch (Exception e) {
                        Panic.panic(e);
                    }
                } else {
                    int mod = pageNumGen.get();
                    if (mod == 0) {
                        continue;
                    }
                    int pageNum = Math.abs(random.nextInt()) % mod + 1;
                    try {
                        Page page = pageCache.getPage(pageNum);
                        page.release();
                    } catch (Exception e) {
                        Panic.panic(e);
                    }
                }
            }
        }
        
        Assert.assertTrue(new File("page-cache-simple-test1.db").delete());
    }
    
    @Test
    public void multiPageCacheTest() throws Exception {
        PageCache pageCache = PageCache.create("./", "page-cache-multi-test", Page.PAGE_SIZE * 100);
        PageCache mockPageCache = new MockPageCache();
        Lock lock = new ReentrantLock();
        CountDownLatch latch = new CountDownLatch(30);
        AtomicInteger pageNumGen = new AtomicInteger(0);

        for (int i = 0; i < 30; i++) {
            Runnable r = () -> {
                for (int j = 0; j < 1000; j++) {
                    int op = Math.abs(random.nextInt() % 20);
                    if (op == 0) {
                        byte[] data = RandomUtils.randomBytes(Page.PAGE_SIZE);
                        lock.lock();
                        int pageNum = pageCache.newPage(data);
                        int mockPageNum = mockPageCache.newPage(data);
                        Assert.assertEquals(pageNum, mockPageNum);
                        lock.unlock();
                        pageNumGen.incrementAndGet();
                    } else if (op < 10) {
                        int mod = pageNumGen.get();
                        if (mod == 0) {
                            continue;
                        }
                        int pageNum = Math.abs(random.nextInt()) % mod + 1;
                        try {
                            Page page = pageCache.getPage(pageNum);
                            Page mockPage = mockPageCache.getPage(pageNum);
                            
                            page.lock();
                            Assert.assertArrayEquals(page.getData(), mockPage.getData());
                            page.unlock();
                            page.release();
                        } catch (Exception e) {
                            Panic.panic(e);
                        }
                    } else {
                        int mod = pageNumGen.get();
                        if (mod == 0) {
                            continue;
                        }
                        int pageNum = Math.abs(random.nextInt()) % mod + 1;
                        try {
                            Page page = pageCache.getPage(pageNum);
                            Page mockPage = mockPageCache.getPage(pageNum);
                            byte[] newData = RandomUtils.randomBytes(Page.PAGE_SIZE);
                            
                            page.lock();
                            page.setDirty(true);
                            for (int k = 0; k < Page.PAGE_SIZE; k++) {
                                page.getData()[k] = newData[k];
                            }
                            mockPage.setDirty(true);
                            for (int k = 0; k < Page.PAGE_SIZE; k++) {
                                mockPage.getData()[k] = newData[k];
                            }
                            page.unlock();
                            page.release();
                        } catch (Exception e) {
                            Panic.panic(e);
                        }
                    }
                }
                latch.countDown();
            };
            new Thread(r).start();
        }
        
        latch.await();
        Assert.assertTrue(new File("page-cache-multi-test.db").delete());
    }
}
