/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.jte.sync2any.util;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The type Collection utils.
 *
 * @author zhangsen
 * @author Geng Zhang
 */
public class CollectionUtils {

    private CollectionUtils() {
    }

    /**
     * Is empty boolean.
     *
     * @param col the col
     * @return the boolean
     */
    public static boolean isEmpty(Collection<?> col) {
        return !isNotEmpty(col);
    }

    /**
     * Is not empty boolean.
     *
     * @param col the col
     * @return the boolean
     */
    public static boolean isNotEmpty(Collection<?> col) {
        return col != null && !col.isEmpty();
    }

    /**
     * Is empty boolean.
     *
     * @param array the array
     * @return the boolean
     */
    public static boolean isEmpty(Object[] array) {
        return !isNotEmpty(array);
    }

    /**
     * Is not empty boolean.
     *
     * @param array the array
     * @return the boolean
     */
    public static boolean isNotEmpty(Object[] array) {
        return array != null && array.length > 0;
    }

    /**
     * Is empty boolean.
     *
     * @param map the map
     * @return the boolean
     */
    public static boolean isEmpty(Map<?, ?> map) {
        return !isNotEmpty(map);
    }

    /**
     * Is not empty boolean.
     *
     * @param map the map
     * @return the boolean
     */
    public static boolean isNotEmpty(Map<?, ?> map) {
        return map != null && !map.isEmpty();
    }


    /**
     * Is size equals boolean.
     *
     * @param col0 the col 0
     * @param col1 the col 1
     * @return the boolean
     */
    public static boolean isSizeEquals(Collection<?> col0, Collection<?> col1) {
        if (col0 == null) {
            return col1 == null;
        } else {
            if (col1 == null) {
                return false;
            } else {
                return col0.size() == col1.size();
            }
        }
    }

    private static final String KV_SPLIT = "=";

    private static final String PAIR_SPLIT = "&";

    /**
     * Encode map to string
     *
     * @param map origin map
     * @return String string
     */
    public static String encodeMap(Map<String, String> map) {
        if (map == null) {
            return null;
        }
        if (map.isEmpty()) {
            return StringUtils.EMPTY;
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            sb.append(entry.getKey()).append(KV_SPLIT).append(entry.getValue()).append(PAIR_SPLIT);
        }
        return sb.substring(0, sb.length() - 1);
    }


    /**
     * Compute if absent.
     * Use this method if you are frequently using the same key,
     * because the get method has no lock.
     *
     * @param map             the map
     * @param key             the key
     * @param mappingFunction the mapping function
     * @param <K>             the type of key
     * @param <V>             the type of value
     * @return the value
     */
    public static <K, V> V computeIfAbsent(Map<K, V> map, K key, Function<? super K, ? extends V> mappingFunction) {
        V value = map.get(key);
        if (value != null) {
            return value;
        }
        return map.computeIfAbsent(key, mappingFunction);
    }

    /**
     * To upper list list.
     *
     * @param sourceList the source list
     * @return the list
     */
    public static List<String> toUpperList(List<String> sourceList) {
        if (isEmpty(sourceList)) {
            return sourceList;
        }
        List<String> destList = new ArrayList<>(sourceList.size());
        for (String element : sourceList) {
            if (element != null) {
                destList.add(element.toUpperCase());
            } else {
                destList.add(null);
            }
        }
        return destList;
    }

    /**
     * Get the last item.
     * <p>
     * 'IndexOutOfBoundsException' may be thrown, because the `list.size()` and `list.get(size - 1)` are not atomic.
     * This method can avoid the 'IndexOutOfBoundsException' cause by concurrency.
     * </p>
     *
     * @param list the list
     * @param <T>  the type of item
     * @return the last item
     */
    public static <T> T getLast(List<T> list) {
        if (isEmpty(list)) {
            return null;
        }

        int size;
        while (true) {
            size = list.size();
            if (size == 0) {
                return null;
            }

            try {
                return list.get(size - 1);
            } catch (IndexOutOfBoundsException ex) {
                // catch the exception and continue to retry
            }
        }
    }
}
