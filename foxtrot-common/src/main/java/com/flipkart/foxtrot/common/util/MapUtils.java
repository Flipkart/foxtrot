package com.flipkart.foxtrot.common.util;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collector;

public class MapUtils {

    private MapUtils() throws IllegalAccessException {
        throw new IllegalAccessException("Utility Class");
    }

    public static <K, V> Collector<Entry<K, V>, ?, List<Map<K, V>>> mapSize(int limit) {
        return Collector.of(ArrayList::new, (l, e) -> {
            addEmptyMap(limit, l);
            l.get(l.size() - 1)
                    .put(e.getKey(), e.getValue());
        }, (l1, l2) -> {
            if (l1.isEmpty()) {
                return l2;
            }
            if (l2.isEmpty()) {
                return l1;
            }
            if (l1.get(l1.size() - 1)
                    .size() < limit) {
                Map<K, V> map = l1.get(l1.size() - 1);
                ListIterator<Map<K, V>> mapsIte = l2.listIterator(l2.size());
                processMap(limit, map, mapsIte);
            }
            l1.addAll(l2);
            return l1;
        });
    }

    private static <K, V> void processMap(int limit,
                                          Map<K, V> map,
                                          ListIterator<Map<K, V>> mapsIte) {
        while (mapsIte.hasPrevious() && map.size() < limit) {
            Iterator<Entry<K, V>> ite = mapsIte.previous()
                    .entrySet()
                    .iterator();

            processNestedMap(limit, map, ite);

            if (!ite.hasNext()) {
                mapsIte.remove();
            }
        }
    }

    private static <K, V> void processNestedMap(int limit,
                                                Map<K, V> map,
                                                Iterator<Entry<K, V>> ite) {
        while (ite.hasNext() && map.size() < limit) {
            Entry<K, V> entry = ite.next();
            map.put(entry.getKey(), entry.getValue());
            ite.remove();
        }
    }

    private static <K, V> void addEmptyMap(int limit,
                                           List<Map<K, V>> l) {
        if (l.isEmpty() || l.get(l.size() - 1)
                .size() == limit) {
            l.add(new HashMap<>());
        }
    }
}
