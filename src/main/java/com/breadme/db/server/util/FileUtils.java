package com.breadme.db.server.util;

import com.breadme.db.common.Error;

import java.io.File;
import java.io.IOException;

public final class FileUtils {
    private FileUtils() {}
    
    public static File create(String dir, String filename) {
        if (!dir.endsWith("/")) {
            dir += "/";
        }
        File file = new File(dir + filename);
        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        
        checkRW(file);
        
        return file;
    }
    
    public static File open(String dir, String filename) {
        if (!dir.endsWith("/")) {
            dir += "/";
        }
        File file = new File(dir + filename);
        if (!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        
        checkRW(file);
        
        return file;
    }
    
    private static void checkRW(File file) {
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
    }
}
