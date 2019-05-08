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

package io.crate.data;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(JUnitQuickcheck.class)
public class LimitingBatchIteratorTest {

    private int limit = 5;
    private List<Object[]> expectedResult = IntStream.range(0, 10).limit(limit)
        .mapToObj(l -> new Object[]{l}).collect(Collectors.toList());

    @Test
    public void testLimitingBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> LimitingBatchIterator.newInstance(TestingBatchIterators.range(0, 10), limit)
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testLimitingBatchIteratorWithBatchedSource() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> {
                BatchSimulatingIterator<Row> batchSimulatingIt = new BatchSimulatingIterator<>(
                    TestingBatchIterators.range(0, 10), 2, 5, null);
                return LimitingBatchIterator.newInstance(batchSimulatingIt, limit);
            }
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Property
    public void testLimitBehavesLikeStreamLimit(ArrayList<Integer> numbers, @InRange(minInt = 0) int limit) throws Exception {
        var it = LimitingBatchIterator.newInstance(InMemoryBatchIterator.of(numbers, null), limit);
        List<Integer> result = BatchIterators.collect(it, Collectors.toList()).get(5, TimeUnit.SECONDS);
        List<Integer> expectedResult = numbers.stream().limit(limit).collect(Collectors.toList());

        assertThat(result, Matchers.is(expectedResult));
    }
}
