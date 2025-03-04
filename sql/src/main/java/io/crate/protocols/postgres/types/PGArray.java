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

package io.crate.protocols.postgres.types;

import com.google.common.primitives.Bytes;
import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

class PGArray extends PGType {

    private final PGType innerType;

    static final PGArray CHAR_ARRAY = new PGArray(1002, CharType.INSTANCE);
    static final PGArray INT2_ARRAY = new PGArray(1005, SmallIntType.INSTANCE);
    static final PGArray INT4_ARRAY = new PGArray(1007, IntegerType.INSTANCE);
    static final PGArray INT8_ARRAY = new PGArray(1016, BigIntType.INSTANCE);
    static final PGArray FLOAT4_ARRAY = new PGArray(1021, RealType.INSTANCE);
    static final PGArray FLOAT8_ARRAY = new PGArray(1022, DoubleType.INSTANCE);
    static final PGArray BOOL_ARRAY = new PGArray(1000, BooleanType.INSTANCE);
    static final PGArray TIMESTAMPZ_ARRAY = new PGArray(1185, TimestampZType.INSTANCE);
    static final PGArray TIMESTAMP_ARRAY = new PGArray(1115, TimestampType.INSTANCE);
    static final PGArray VARCHAR_ARRAY = new PGArray(1015, VarCharType.INSTANCE);
    static final PGArray JSON_ARRAY = new PGArray(199, JsonType.INSTANCE);
    static final PGArray POINT_ARRAY = new PGArray(1017, PointType.INSTANCE);

    private static final byte[] NULL_BYTES = new byte[]{'N', 'U', 'L', 'L'};

    private PGArray(int oid, PGType innerType) {
        super(oid, -1, -1, "_" + innerType.typName());
        this.innerType = innerType;
    }

    @Override
    public int typArray() {
        return 0;
    }

    @Override
    public int typElem() {
        return innerType.oid();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull Object value) {
        int dimensions = getDimensions(value);

        List<Integer> dimensionsList = new ArrayList<>();
        buildDimensions((List<Object>) value, dimensionsList, dimensions, 1);

        int bytesWritten = 4 + 4 + 4;
        final int lenIndex = buffer.writerIndex();
        buffer.writeInt(0);
        buffer.writeInt(dimensions);
        buffer.writeInt(1); // flags bit 0: 0=no-nulls, 1=has-nulls
        buffer.writeInt(typElem());

        for (Integer dim : dimensionsList) {
            buffer.writeInt(dim); // upper bound
            buffer.writeInt(dim); // lower bound
            bytesWritten += 8;
        }
        int len = bytesWritten + writeArrayAsBinary(buffer, (List<Object>) value, dimensionsList, 1);
        buffer.setInt(lenIndex, len);
        return INT32_BYTE_SIZE + len; // add also the size of the length itself
    }

    private int getDimensions(@Nonnull Object value) {
        int dimensions = 0;
        Object array = value;

        do {
            dimensions++;
            List arr = (List) array;
            if (arr.isEmpty()) {
                break;
            }
            array = null;
            for (Object o : arr) {
                if (o == null) {
                    continue;
                }
                array = o;
            }
        } while (array instanceof List);
        return dimensions;
    }

    @Override
    public Object readBinaryValue(ByteBuf buffer, int valueLength) {
        int dimensions = buffer.readInt();
        buffer.readInt(); // flags bit 0: 0=no-nulls, 1=has-nulls
        buffer.readInt(); // element oid
        if (dimensions == 0) {
            return new Object[0];
        }
        int[] dims = new int[dimensions];
        for (int d = 0; d < dimensions; ++d) {
            dims[d] = buffer.readInt();
            buffer.readInt(); // lowerBound ignored
        }
        List<Object> values = new ArrayList<>(dims[0]);
        readArrayAsBinary(buffer, values, dims, 0);
        return values;
    }

    @Override
    byte[] encodeAsUTF8Text(@Nonnull Object array) {
        boolean isJson = JsonType.OID == innerType.oid();
        List<Object> values = (List<Object>) array;
        List<Byte> encodedValues = new ArrayList<>();
        encodedValues.add((byte) '{');
        for (int i = 0; i < values.size(); i++) {
            Object o = values.get(i);
            if (o instanceof List) { // Nested Array -> recursive call
                byte[] bytes = encodeAsUTF8Text(o);
                for (byte b : bytes) {
                    encodedValues.add(b);
                }
                if (i == 0) {
                    encodedValues.add((byte) ',');
                }
            } else {
                if (i > 0) {
                    encodedValues.add((byte) ',');
                }
                byte[] bytes;
                if (o == null) {
                    bytes = NULL_BYTES;
                    for (byte aByte : bytes) {
                        encodedValues.add(aByte);
                    }
                } else {
                    bytes = innerType.encodeAsUTF8Text(o);

                    encodedValues.add((byte) '"');
                    if (isJson) {
                        for (byte aByte : bytes) {
                            // Escape double quotes with backslash for json
                            if ((char) aByte == '"') {
                                encodedValues.add((byte) '\\');
                            }
                            encodedValues.add(aByte);
                        }
                    } else {
                        for (byte aByte : bytes) {
                            encodedValues.add(aByte);
                        }
                    }
                    encodedValues.add((byte) '"');
                }
            }
        }
        encodedValues.add((byte) '}');
        return Bytes.toArray(encodedValues);
    }

    @Override
    Object decodeUTF8Text(byte[] bytes) {
        /*
         * text representation:
         *
         * 1-dimension integer array:
         *      {10,NULL,NULL,20,30}
         *      {"10",NULL,NULL,"20","30"}
         * 2-dimension integer array:
         *      {{"10","20"},{"30",NULL,"40}}
         *
         * 1-dimension json array:
         *      {"{"x": 10}","{"y": 20}"}
         * 2-dimension json array:
         *      {{"{"x": 10}","{"y": 20}"},{"{"x": 30}","{"y": 40}"}}
         */

        List<Object> values = new ArrayList<>();
        decodeUTF8Text(bytes, 0, bytes.length - 1, values);
        return values;
    }

    private int decodeUTF8Text(byte[] bytes, int startIdx, int endIdx, List<Object> objects) {
        int valIdx = startIdx;
        boolean jsonObject = false;
        for (int i = startIdx; i <= endIdx; i++) {
            byte aByte = bytes[i];
            switch (aByte) {
                case '{':
                    if (i == 0 || bytes[i - 1] != '"') {
                        if (i > 0) {
                            // n-dimensions array -> call recursively
                            List<Object> nestedObjects = new ArrayList<>();
                            i = decodeUTF8Text(bytes, i + 1, endIdx, nestedObjects);
                            valIdx = i + 1;
                            objects.add(nestedObjects);
                        } else {
                            // 1-dimension array -> call recursively
                            i = decodeUTF8Text(bytes, i + 1, endIdx, objects);
                        }
                    } else {
                        // Start of JSON object
                        valIdx = i - 1;
                        jsonObject = true;
                    }
                    break;
                case ',':
                    if (!jsonObject) {
                        addObject(bytes, valIdx, i - 1, objects);
                        valIdx = i + 1;
                    }
                    break;
                case '}':
                    if (i == endIdx || bytes[i + 1] != '"') {
                        addObject(bytes, valIdx, i - 1, objects);
                        return i;
                    } else {
                        //End of JSON object
                        addObject(bytes, valIdx, i + 1, objects);
                        jsonObject = false;
                        i++;
                        valIdx = i;
                        if (bytes[i] == '}') { // end of array
                            return i + 2;
                        }
                    }
                // fall through
                default:
            }
        }
        return endIdx;
    }

    // Decode individual inner object
    private void addObject(byte[] bytes, int startIdx, int endIdx, List<Object> objects) {
        if (endIdx >= startIdx) {
            byte firstValueByte = bytes[startIdx];
            if (firstValueByte == 'N') {
                objects.add(null);
            } else {
                if (firstValueByte == '"') {
                    // skip any quote character
                    if (startIdx == endIdx) {
                        return;
                    }
                    startIdx++;
                    endIdx--;
                }
                byte[] innerBytes = new byte[endIdx - startIdx + 1];
                for (int i = startIdx, innerBytesIdx = 0; i <= endIdx; i++, innerBytesIdx++) {
                    if (i < endIdx && (char) bytes[i] == '\\' &&
                        ((char) bytes[i + 1] == '\\' || (char) bytes[i + 1] == '\"')) {
                        i++;
                    }
                    innerBytes[innerBytesIdx] = bytes[i];
                }
                objects.add(innerType.decodeUTF8Text(innerBytes));
            }
        }
    }

    private int buildDimensions(List<Object> values, List<Integer> dimensionsList, int maxDimensions, int currentDimension) {
        if (values == null) {
            return 1;
        }
        // While elements of array are also arrays
        if (currentDimension < maxDimensions) {
            int max = 0;
            for (Object o : values) {
                max = Math.max(max, buildDimensions((List<Object>) o, dimensionsList, maxDimensions, currentDimension + 1));
            }

            if (currentDimension == maxDimensions - 1) {
                dimensionsList.add(max);
            } else {
                Integer current = dimensionsList.get(0);
                dimensionsList.set(0, Math.max(current, max));
            }
        }
        // Add the dimensions of 1st dimension
        if (currentDimension == 1) {
            dimensionsList.add(0, values.size());
        }
        return values.size();
    }

    private int writeArrayAsBinary(ByteBuf buffer, List<Object> array, List<Integer> dimensionsList, int currentDimension) {
        int bytesWritten = 0;

        if (array == null) {
            for (int i = 0; i < dimensionsList.get(currentDimension - 1); i++) {
                buffer.writeInt(-1);
                bytesWritten += 4;
            }
            return bytesWritten;
        }

        // 2nd to last level
        if (currentDimension == dimensionsList.size()) {
            int i = 0;
            for (Object o : array) {
                if (o == null) {
                    buffer.writeInt(-1);
                    bytesWritten += 4;
                } else {
                    bytesWritten += innerType.writeAsBinary(buffer, o);
                }
                i++;
            }
            // Fill in with -1 for up to max dimensions
            for (; i < dimensionsList.get(currentDimension - 1); i++) {
                buffer.writeInt(-1);
                bytesWritten += 4;
            }
        } else {
            for (Object o : array) {
                bytesWritten += writeArrayAsBinary(buffer, (List<Object>) o, dimensionsList, currentDimension + 1);
            }
        }
        return bytesWritten;
    }

    private void readArrayAsBinary(ByteBuf buffer,
                                   final List<Object> array,
                                   final int[] dims,
                                   final int thisDimension) {
        if (thisDimension == dims.length - 1) {
            for (int i = 0; i < dims[thisDimension]; ++i) {
                int len = buffer.readInt();
                if (len == -1) {
                    array.add(null);
                } else {
                    array.add(innerType.readBinaryValue(buffer, len));
                }
            }
        } else {
            for (int i = 0; i < dims[thisDimension]; ++i) {
                ArrayList<Object> list = new ArrayList<>(dims[thisDimension + 1]);
                array.add(list);
                readArrayAsBinary(buffer, list, dims, thisDimension + 1);
            }
        }
    }
}
