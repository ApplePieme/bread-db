package com.breadme.db.server.tm;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerTest {
    private static final Random random = new SecureRandom();
    private int transCnt = 0;
    private int numWorkers = 50;
    private int numWorks = 3000;
    private final Lock lock = new ReentrantLock();
    private TransactionManager tm;
    private Map<Long, Byte> transMap;
    private CountDownLatch latch;
    
    @Test
    public void multiThreadTest() {
        tm = TransactionManager.create("tm.xid");
        transMap = new ConcurrentHashMap<>();
        latch = new CountDownLatch(numWorkers);
        
        for (int i = 0; i < numWorkers; i++) {
            Runnable r = this::worker;
            new Thread(r).start();
        }
        
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        Assert.assertTrue(new File("tm.xid").delete());
    }
    
    private void worker() {
        boolean inTrans = false;
        long transXid = 0;
        for (int i = 0; i < numWorks; i++) {
            int op = Math.abs(random.nextInt(6));
            lock.lock();
            if (op == 0) {
                if (!inTrans) {
                    long xid = tm.begin();
                    transMap.put(xid, (byte) 0);
                    ++transCnt;
                    transXid = xid;
                    inTrans = true;
                } else {
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch (status) {
                        case 1:
                            tm.commit(transXid);
                            break;
                        case 2:
                            tm.abort(transXid);
                            break;
                    }
                    transMap.put(transXid, (byte) status);
                    inTrans = false;
                }
            } else {
                if (transCnt > 0) {
                    long xid = (random.nextInt(Integer.MAX_VALUE) % transCnt) + 1;
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = tm.isActive(xid);
                            break;
                        case 1:
                            ok = tm.isCommitted(xid);
                            break;
                        case 2:
                            ok = tm.isAborted(xid);
                            break;
                    }
                    Assert.assertTrue(ok);
                }
            }
            lock.unlock();
        }
        latch.countDown();
    }
}
