package com.breadme.db.server.common;

import com.breadme.db.common.Error;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {
    private final HashMap<Long, T> cache = new HashMap<>();
    private final HashMap<Long, Integer> references = new HashMap<>();
    private final HashMap<Long, Boolean> getting = new HashMap<>();
    private final Lock lock = new ReentrantLock();
    private final int capacity;
    private int size;
    
    public AbstractCache(int capacity) {
        this.capacity = capacity;
    }
    
    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            
            if (cache.containsKey(key)) {
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }
            
            if (capacity > 0 && size == capacity) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            
            ++size;
            getting.put(key, true);
            lock.unlock();
            break;
        }
        
        T obj;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            --size;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();
        
        return obj;
    }
    
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                --size;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }
    
    protected void close() {
        lock.lock();
        try {
            for (long key : cache.keySet()) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                --size;
            }
        } finally {
            lock.unlock();
        }
    }
    
    protected abstract T getForCache(long key) throws Exception;
    
    protected abstract void releaseForCache(T obj);
}
