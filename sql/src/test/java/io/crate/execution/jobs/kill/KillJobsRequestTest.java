/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.jobs.kill;


import com.google.common.collect.ImmutableList;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;

public class KillJobsRequestTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        ImmutableList<UUID> toKill = ImmutableList.of(UUID.randomUUID(), UUID.randomUUID());
        KillJobsRequest r = new KillJobsRequest(toKill);

        BytesStreamOutput out = new BytesStreamOutput();
        r.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        KillJobsRequest r2 = new KillJobsRequest(in);

        assertThat(r.toKill(), equalTo(r2.toKill()));
    }
}
