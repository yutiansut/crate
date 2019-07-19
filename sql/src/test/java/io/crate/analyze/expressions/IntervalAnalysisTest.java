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

package io.crate.analyze.expressions;

import io.crate.expression.symbol.Symbol;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.Interval;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class IntervalAnalysisTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (ts timestamp without time zone)")
            .build();
    }

    @Test
    public void test_interval_from_ISO8601_year_month_day() {
        var symbol = e.asSymbol("INTERVAL 'P1Y2M3W4D'");
        assertThat(symbol, isLiteral(new Interval(0, 25, 14)));
    }

    @Test
    public void test_interval_from_ISO8601_year_month_day_and_time() {
        var symbol = e.asSymbol("INTERVAL 'P1Y2M3DT4H5M6S'");
        assertThat(symbol, isLiteral(new Interval(0, 25, 14)));
    }

    @Test
    public void test_interval_from_human_readable_duration_string() {
        var symbol = e.asSymbol("INTERVAL '6 years 5 months 4 days 3 hours 2 minutes 1 second'");
        assertThat(symbol, isLiteral(new Interval(0, 25, 14)));
    }

    @Test
    public void testInterval() throws Exception {
        Symbol symbol = e.asSymbol("INTERVAL '1' MONTH");
        assertThat(symbol, isLiteral(new Interval(0, 0, 1)));
    }

    @Test
    public void testIntervalConversion() throws Exception {
        Symbol symbol =  e.asSymbol("INTERVAL '1' HOUR to SECOND");
        assertThat(symbol, isLiteral(new Interval(1, 0, 0)));
    }

    @Test
    public void testIntervalInvalidStartEnd() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Startfield MONTH must be less significant than Endfield YEAR");
        e.asSymbol("INTERVAL '1' MONTH TO YEAR");
    }
}
