package com.breadme.db.server.util;

import java.nio.ByteBuffer;

public final class Parser {
    private Parser() {}
    
    public static long parseLong(byte[] buf) {
        return ByteBuffer.wrap(buf, 0, 8).getLong();
    }
    
    public static short parseShort(byte[] buf) {
        return ByteBuffer.wrap(buf, 0, 2).getShort();
    }
    
    public static byte[] short2Bytes(short val) {
        return ByteBuffer.allocate(2).putShort(val).array();
    }
}
