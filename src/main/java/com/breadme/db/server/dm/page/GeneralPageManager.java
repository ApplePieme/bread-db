package com.breadme.db.server.dm.page;

import com.breadme.db.server.util.Parser;

import java.util.Arrays;

/**
 * 普通页管理器
 * 
 * 普通页结构:
 * [FreeSpacePosition][Data]
 * FreeSpacePosition: 长度为2字节, 用于记录空闲空间的位置
 */
public final class GeneralPageManager {
    private static final short FREE_SPACE_POS_OFFSET = 0;
    private static final short FREE_SPACE_POS_LEN = 2;
    private static final int MAX_FREE_SPACE = Page.PAGE_SIZE - FREE_SPACE_POS_LEN;
    
    private GeneralPageManager() {}
    
    public static byte[] initRaw() {
        byte[] raw = new byte[Page.PAGE_SIZE];
        setFreeSpacePosition(raw, FREE_SPACE_POS_LEN);
        return raw;
    }
    
    public static short getFreeSpacePosition(Page page) {
        return getFreeSpacePosition(page.getData());
    }
    
    public static short insert(Page page, byte[] raw) {
        page.setDirty(true);
        short pos = getFreeSpacePosition(page.getData());
        System.arraycopy(raw, 0, page.getData(), pos, raw.length);
        setFreeSpacePosition(page.getData(), (short) (pos + raw.length));
        return pos;
    }
    
    public static int getFreeSpace(Page page) {
        return Page.PAGE_SIZE - (int) getFreeSpacePosition(page.getData());
    }
    
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        
        short pos = getFreeSpacePosition(page.getData());
        if (pos < offset + raw.length) {
            setFreeSpacePosition(page.getData(), (short) (offset + raw.length));
        }
    }
    
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
    }
    
    private static void setFreeSpacePosition(byte[] raw, short dataOffset) {
        System.arraycopy(Parser.short2Bytes(dataOffset), 0, raw, FREE_SPACE_POS_OFFSET, FREE_SPACE_POS_LEN);
    }
    
    private static short getFreeSpacePosition(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }
}
