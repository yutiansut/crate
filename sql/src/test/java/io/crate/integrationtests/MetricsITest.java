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

package io.crate.integrationtests;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import io.crate.concurrent.CompletableFutures;
import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.metadata.sys.MetricsView;
import io.crate.planner.Plan;
import io.crate.testing.SQLResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.Is.is;

public class MetricsITest extends SQLTransportIntegrationTest {

    @Before
    public void clearStats() {
        execute("SET GLOBAL \"stats.enabled\" = FALSE");
        execute("SET GLOBAL \"stats.enabled\" = TRUE");
    }

    @After
    public void resetStats() {
        execute("RESET GLOBAL \"stats.enabled\"");
    }

    @Test
    public void testSimpleQueryOnMetrics() {
        execute("SELECT 1");

        execute("SELECT node, " +
                "node['id'], " +
                "node['name'], " +
                "min, " +
                "max, " +
                "mean, " +
                "percentiles, " +
                "percentiles['25'], " +
                "percentiles['50'], " +
                "percentiles['75'], " +
                "percentiles['90'], " +
                "percentiles['95'], " +
                "percentiles['99'], " +
                "total_count, " +
                "classification " +
                "FROM sys.jobs_metrics " +
                "WHERE classification['type'] = 'SELECT' " +
                "ORDER BY max DESC");

        for (Object[] row : response.rows()) {
            assertThat((long) row[3], Matchers.greaterThanOrEqualTo(0L));
            assertThat((long) row[4], Matchers.greaterThanOrEqualTo(0L));
            assertThat((double) row[5], Matchers.greaterThanOrEqualTo(0.0d));
            assertThat(row[13], Matchers.is(1L));
        }
    }

    @Test
    public void testTotalCountOnMetrics() throws Exception {
        int numQueries = 100;
        for (int i = 0; i < numQueries; i++) {
            execute("SELECT 1");
        }

        // We record the data for the histograms **after** we notify the result receivers
        // (see {@link JobsLogsUpdateListener usage).
        // So it might happen that the recording of the statement the "SELECT 1" in the metrics is done
        // AFTER its execution has returned to this test (because async programming is evil like that).
        assertBusy(() -> {
            long cnt = 0;
            for (JobsLogService jobsLogService : internalCluster().getInstances(JobsLogService.class)) {
                for (MetricsView metrics: jobsLogService.get().metrics()) {
                    if (metrics.classification().type() == Plan.StatementType.SELECT) {
                        cnt += metrics.histogram().getTotalCount();
                    }
                }
            }
            assertThat(cnt, is((long) numQueries));
        });

        execute("SELECT sum(total_count) FROM sys.jobs_metrics WHERE classification['type'] = 'SELECT'");
        assertThat(response.rows()[0][0], Matchers.is((long) numQueries));
    }

    @Test
    @Repeat (iterations = 500)
    public void testSysJobsMetricsIsGuardedAgainstConcurrentModifications() throws Exception{
        int concurrency = 25;
        var threadPool = internalCluster().getInstance(ThreadPool.class);
        var executor = threadPool.executor(ThreadPool.Names.GENERIC);
        var running = new AtomicBoolean(true);
        var threadsFinished = new CountDownLatch(concurrency);
        for (int i = 0; i < concurrency; i++) {
            executor.execute(() -> {
                while (running.get()) {
                    execute("select 1");
                }
                threadsFinished.countDown();
            });
        }
        var responses = new ArrayList<CompletableFuture<SQLResponse>>();
        for (int i = 0; i < 50; i++) {
            responses.add(CompletableFuture.supplyAsync(
                () -> execute("select sum(failed_count) from sys.jobs_metrics"),
                threadPool.executor(ThreadPool.Names.GET)));
        }
        for (SQLResponse sqlResponse : CompletableFutures.allAsList(responses).get(5, TimeUnit.SECONDS)) {
            assertThat(sqlResponse.rows()[0][0], is(0L));
        }
        running.set(false);
        threadsFinished.await(5, TimeUnit.SECONDS);
    }
}
