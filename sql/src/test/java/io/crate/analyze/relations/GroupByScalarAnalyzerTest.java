package io.crate.analyze.relations;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;


public class GroupByScalarAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void prepare() throws IOException {
        executor = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testScalarFunctionArgumentsNotAllInGroupByThrowsException() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'(id * other_id)' must appear in the GROUP BY");
        executor.analyze("select id * other_id from users group by id");
    }

    @Test
    public void testValidGroupByWithScalarAndMultipleColumns() throws Exception {
        AnalyzedRelation relation = executor.analyze("select id * other_id from users group by id, other_id");
        assertThat(relation.fields().get(0), isFunction("multiply", isReference("id"), isReference("other_id")));
    }

    @Test
    public void testValidGroupByWithScalar() throws Exception {
        AnalyzedRelation relation = executor.analyze("select id * 2 from users group by id");
        assertThat(relation.fields().get(0), isFunction("multiply", isReference("id"), isLiteral(2L)));
    }

    @Test
    public void testValidGroupByWithMultipleScalarFunctions() throws Exception {
        AnalyzedRelation relation = executor.analyze("select abs(id * 2) from users group by id");
        assertThat(relation.fields().get(0), isFunction("abs", isFunction("multiply", isReference("id"), isLiteral(2L))));
    }
}
