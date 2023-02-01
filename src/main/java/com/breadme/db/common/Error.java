package com.breadme.db.common;

public class Error {
    // common
    public static final Exception CacheFullException = new RuntimeException("cache is full");
    public static final Exception FileExistsException = new RuntimeException("file already exists");
    public static final Exception FileNotExistsException = new RuntimeException("file does not exists");
    public static final Exception FileCannotRWException = new RuntimeException("file cannot read or write");
    
    // tm
    public static final Exception BadXidFileException = new RuntimeException("bad xid file");
}
