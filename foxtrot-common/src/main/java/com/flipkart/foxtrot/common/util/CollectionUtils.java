package com.flipkart.foxtrot.common.util;

import java.util.List;
import java.util.Set;

/**
 * Created by rishabh.goyal on 15/01/16.
 */
public class CollectionUtils {

    public static boolean isNullOrEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }

    public static boolean isNullOrEmpty(List list) {
        return list == null || list.isEmpty();
    }

    public static boolean isNullOrEmpty(Set set) {
        return set == null || set.isEmpty();
    }


}
