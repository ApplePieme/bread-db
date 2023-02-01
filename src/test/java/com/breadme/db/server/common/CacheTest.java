package com.breadme.db.server.common;

import com.breadme.db.common.Error;
import com.breadme.db.server.util.Panic;
import org.junit.Assert;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class CacheTest {
    private static final Random random = new SecureRandom();
    private static final int numWorkers = 200;
    private static final int numWorks = 1000;
    private static final int cacheCapacity = 50;
    private CountDownLatch latch;
    private MockCache cache;
    
    @Test
    public void cacheTest() {
        latch = new CountDownLatch(numWorkers);
        cache = new MockCache(cacheCapacity);
        
        for (int i = 0; i < numWorkers; i++) {
            Runnable r = this::work;
            new Thread(r).start();
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    private void work() {
        for (int i = 0; i < numWorks; i++) {
            long id = random.nextInt();
            long val = 0;
            try {
                val = cache.get(id);
            } catch (Exception e) {
                if (e == Error.CacheFullException) {
                    continue;
                }
                Panic.panic(e);
            }
            Assert.assertEquals(id, val);
            cache.release(id);
        }
        latch.countDown();
    }
}
