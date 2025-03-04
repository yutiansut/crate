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

package io.crate.execution.engine.window;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

import static io.crate.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.crate.sql.tree.WindowFrame.Mode.RANGE;
import static org.hamcrest.core.Is.is;

public class UnboundedFollowingFrameBoundTest extends CrateUnitTest {

    private List<Integer> partition;
    private Comparator<Integer> intComparator;

    @Before
    public void setupPartitionAndComparator() {
        intComparator = Comparator.comparing(x -> x);
        partition = List.of(1, 2, 2);
    }

    @Test
    public void testEndForFirstFrame() {
        var partition = List.of(1, 2, 2);
        int end = UNBOUNDED_FOLLOWING.getEnd(RANGE, 0, 3, 0, null, null, intComparator, partition);
        assertThat("the end boundary should always be the end of the partition for the UNBOUNDED FOLLOWING frames",
                   end,
                   is(3));
    }

    @Test
    public void testEndForSecondFrame() {
        int end = UNBOUNDED_FOLLOWING.getEnd(RANGE, 0, 3, 1, null, null, intComparator, partition);
        assertThat("the end boundary should always be the end of the partition for the UNBOUNDED FOLLOWING frames",
                   end,
                   is(3));
    }

    @Test
    public void testUnboundeFollowingCannotBeTheStartOfTheFrame() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("UNBOUNDED FOLLOWING cannot be the start of a frame");
        UNBOUNDED_FOLLOWING.getStart(RANGE, 0, 3, 1, null, null, intComparator, partition);
    }

}
