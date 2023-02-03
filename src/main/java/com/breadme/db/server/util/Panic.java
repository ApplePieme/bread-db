package com.breadme.db.server.util;

public final class Panic {
    private Panic() {}
    
    public static void panic(Exception e) {
        e.printStackTrace();
        System.exit(-1);
    }
}
