package com.flipkart.foxtrot.core.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StorageSizeUtils {

    public static String humanReadableByteCountSI(long bytes) {
        if (-1000 < bytes && bytes < 1000) {
            return bytes + " B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.1f %cB", bytes / 1000.0, ci.current());
    }

    public static double bytesToGigaBytes(long bytes) {
        return bytes / 1000000000.0;
    }

    public static double bytesToTeraBytes(long bytes) {
        return bytes / 1000000000000.0;
    }

    public static double bytesToMegaBytes(long bytes) {
        return bytes / 1000000.0;
    }

    public static double bytesToKiloBytes(long bytes) {
        return bytes / 1000.0;
    }
}
