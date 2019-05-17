/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;

public class Maps {

    /**
     * Adds an entry to an immutable map by copying the underlying map and adding the new entry. This method expects there is not already a
     * mapping for the specified key in the map.
     *
     * @param map   the immutable map to concatenate the entry to
     * @param key   the key of the new entry
     * @param value the value of the entry
     * @param <K>   the type of the keys in the map
     * @param <V>   the type of the values in the map
     * @return an immutable map that contains the items from the specified map and the concatenated entry
     */
    public static <K, V> Map<K, V> copyMapWithAddedEntry(final Map<K, V> map, final K key, final V value) {
        Objects.requireNonNull(map);
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        assert map.containsKey(key) == false : "expected entry [" + key + "] to not already be present in map";
        return Stream.concat(map.entrySet().stream(), Stream.of(entry(key, value)))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
