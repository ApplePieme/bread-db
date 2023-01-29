package com.breadme.db.server.util;

public class Panic {
    public static void panic(Exception e) {
        e.printStackTrace();
        System.exit(-1);
    }
}
