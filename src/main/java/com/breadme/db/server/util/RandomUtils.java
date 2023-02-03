package com.breadme.db.server.util;

import java.security.SecureRandom;
import java.util.Random;

public final class RandomUtils {
    private RandomUtils() {}
    
    public static byte[] randomBytes(int len) {
        Random random = new SecureRandom();
        byte[] buf = new byte[len];
        random.nextBytes(buf);
        return buf;
    }
}
