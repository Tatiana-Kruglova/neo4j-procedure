package org.neo4j.procedure;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.impl.proc.JarBuilder;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by Tatiana Chukina
 */
public class NodeAndRelationsCreatorProcedureTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule().withProcedure(NodeAndRelationsCreatorProcedure.class);
    protected GraphDatabaseService graphDb;
    @Rule
    public TemporaryFolder plugins = new TemporaryFolder();

    @Before
    public void prepareTestDatabase() throws IOException {
        new JarBuilder().createJarFor(plugins.newFile("myProcedures.jar"), NodeAndRelationsCreatorProcedure.class);
        graphDb = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.plugin_dir, plugins.getRoot().getAbsolutePath())
                .newGraphDatabase();
    }

    @After
    public void destroyTestDatabase() {
        graphDb.shutdown();
    }

    @Test
    public void testCreateNodeAndRelation() throws Throwable {
        String names = "['A','B', 'C', 'D']";
        String query = String.format("CALL createNodesAndRelations(%s, 5)", names);
        Result execute = graphDb.execute(query);
        System.out.println("PROCEDURE RESULT:");
        Map<String, Object> actual = execute.next();
        System.out.println(actual.keySet() + " " + actual.values());
        printAfterProcedure();
        assertEquals("Expected true found false in " + query, true, actual.get("result"));
        countNumberOfNodesAndRelations(names, query, 4l, 5l);
    }

    @Test
    public void testShouldNotCreateCycles() throws Throwable {
        String names = "['A','B', 'C', 'D']";
        String query = String.format("CALL createNodesAndRelations(%s, 1)", names);
        Result execute = graphDb.execute(query);
        System.out.println("PROCEDURE RESULT:");
        Map<String, Object> actual = execute.next();
        System.out.println(actual.keySet() + " " + actual.values());
        printAfterProcedure();
        assertEquals("Expected false found true in " + query, false, actual.get("result"));
        countNumberOfNodesAndRelations(names, query, 4l, 1l);
    }

    @Test
    public void testShouldCreateNone() throws Throwable {
        String names = "[]";
        String query = String.format("CALL createNodesAndRelations(%s, 0)", names);
        Result execute = graphDb.execute(query);
        System.out.println("PROCEDURE RESULT:");
        Map<String, Object> actual = execute.next();
        System.out.println(actual.keySet() + " " + actual.values());
        printAfterProcedure();
        assertEquals("Expected false found true in " + query, false, actual.get("result"));
        countNumberOfNodesAndRelations(names, query, 0, 0);
    }

    @Test
    public void testInvalidRelationsNum() throws Throwable {
        String names = "['A','B', 'C', 'D']";
        String query = String.format("CALL createNodesAndRelations(%s, -1)", names);
        Result execute = graphDb.execute(query);
        System.out.println("PROCEDURE RESULT:");
        Map<String, Object> actual = execute.next();
        System.out.println(actual.keySet() + " " + actual.values());
        printAfterProcedure();
        assertEquals("Expected false found true in " + query, false, actual.get("result"));
        countNumberOfNodesAndRelations(names, query, 4, 0);
    }

    @Test
    public void testShouldCreateSingleNodeManyRelations() throws Throwable {
        String names = "['A']";
        String query = String.format("CALL createNodesAndRelations(%s, 10)", names);
        Result execute = graphDb.execute(query);
        System.out.println("PROCEDURE RESULT:");
        Map<String, Object> actual = execute.next();
        System.out.println(actual.keySet() + " " + actual.values());
        printAfterProcedure();
        assertEquals("Expected true found false in " + query, true, actual.get("result"));
        countNumberOfNodesAndRelations(names, query, 1, 10);
    }

    private void printAfterProcedure() {
        System.out.println("AFTER PROCEDURE:");
        Result afterProcedure = graphDb.execute("START n=node(*) OPTIONAL MATCH (n)-[r]->(m) RETURN n,r,m;");
        System.out.println(afterProcedure.resultAsString());
    }

    private void countNumberOfNodesAndRelations(String names, String query, long nodeNumbersExpected, long relationsNumberExpected) {
        Result countNodesProcedure = graphDb.execute(String.format("MATCH (n:node) where n.name in %s return count(n) as result", names));
        assertEquals("Unexpected nodes count for " + query, nodeNumbersExpected, countNodesProcedure.next().get("result"));
        Result countRelationsProcedure = graphDb.execute(String.format("START n=node(*)  MATCH (n:node)-[r:RELATION]->(m:node)  where n.name in %s and m.name in %s RETURN count(r) as result;", names, names));
        assertEquals("Unexpected relations count for " + query, relationsNumberExpected, countRelationsProcedure.next().get("result"));
    }

}