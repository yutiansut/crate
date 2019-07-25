/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;

import java.util.Locale;
import java.util.Map;

public class IndexMethod {

    public static final IndexMethod PLAIN = new IndexMethod(Reference.IndexType.PLAIN, Map.of());

    private final Reference.IndexType indexType;
    private final Map<String, Symbol> properties;

    public static IndexMethod of(String indexMethod, Map<String, Symbol> properties) {
        switch (indexMethod.toLowerCase(Locale.ENGLISH)) {
            case "fulltext":
                return new IndexMethod(Reference.IndexType.ANALYZED, properties);

            case "plain":
                if (!properties.isEmpty()) {
                    throw new IllegalArgumentException(
                        "PLAIN index option doesn't support any properties, got: " + properties.keySet());
                }
                return PLAIN;

            case "off":
                if (!properties.isEmpty()) {
                    throw new IllegalArgumentException(
                        "OFF index option doesn't support any properties, got: " + properties.keySet());
                }
                return new IndexMethod(Reference.IndexType.OFF, Map.of());

            case "quadtree":
            case "geohash":
                return new IndexMethod(Reference.IndexType.PLAIN, properties);


            default:
                throw new IllegalArgumentException("Invalid index method: " + indexMethod);
        }
    }

    public IndexMethod(Reference.IndexType indexType, Map<String, Symbol> properties) {
        this.indexType = indexType;
        this.properties = properties;
    }
}
