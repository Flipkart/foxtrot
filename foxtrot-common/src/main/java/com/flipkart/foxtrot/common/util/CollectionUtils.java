package com.flipkart.foxtrot.common.util;

import java.util.Collection;

/**
 * Created by rishabh.goyal on 15/01/16.
 */
public class CollectionUtils {

    public static boolean isNullOrEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }

    public static boolean isNullOrEmpty(Collection list) {
        return list == null || list.isEmpty();
    }

}
