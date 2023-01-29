package com.breadme.db.server.util;

import java.nio.ByteBuffer;

public class Parser {
    public static long parseLong(byte[] buf) {
        return ByteBuffer.wrap(buf, 0, 8).getLong();
    }
}
