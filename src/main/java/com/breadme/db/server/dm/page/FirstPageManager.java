package com.breadme.db.server.dm.page;

import com.breadme.db.server.util.RandomUtils;

import java.util.Arrays;

/**
 * 特殊页管理器
 * 第一页为特殊页, 主要用于检查数据库是否正常关闭
 */
public final class FirstPageManager {
    private static final int VALID_CHECK_OFFSET = 100;
    private static final int VALID_CHECK_LEN = 8;
    
    private FirstPageManager() {}
    
    public static byte[] initRaw() {
        byte[] raw = new byte[Page.PAGE_SIZE];
        setValidCheckOpen(raw);
        return raw;
    }
    
    public static void setValidCheckOpen(Page page) {
        page.setDirty(true);
        setValidCheckOpen(page.getData());
    }
    
    public static void setValidCheckClose(Page page) {
        page.setDirty(true);
        setValidCheckClose(page.getData());
    }
    
    public static boolean checkValidity(Page page) {
        return checkValidity(page.getData());
    }
    
    private static void setValidCheckOpen(byte[] raw) {
        System.arraycopy(RandomUtils.randomBytes(VALID_CHECK_LEN), 0, raw, VALID_CHECK_OFFSET, VALID_CHECK_LEN);
    }
    
    private static void setValidCheckClose(byte[] raw) {
        System.arraycopy(raw, VALID_CHECK_OFFSET, raw, VALID_CHECK_OFFSET + VALID_CHECK_LEN, VALID_CHECK_LEN);
    }
    
    private static boolean checkValidity(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, VALID_CHECK_OFFSET, VALID_CHECK_LEN), Arrays.copyOfRange(raw, VALID_CHECK_OFFSET + VALID_CHECK_LEN, VALID_CHECK_LEN));
    }
}
