package org.neo4j.procedure;

import org.neo4j.graphdb.*;

import java.util.*;
import java.util.stream.Stream;

/**
 * Created by Tatiana Chukina
 */
public class NodeAndRelationsCreatorProcedure {
    public static final String CYCLES_QUERY = "MATCH path = (e:node)-[:RELATION*]->(e:node) where ID(e) in {nodesids} RETURN count(e) > 0 as result";
    @Context
    public GraphDatabaseService db;

    /**
     * Neo4j procedure createNodesAndRelations(<node_names_array>, N),
     * where<node_names_array> - the list of nodes to create.
     * N - the number of directional relations to create between random nodes.
     * <p>
     * Procedure should return value is there any cycles created with function.
     * <p>
     * For example
     * createNodesAndRelations(["A", "B", "C", "D"], 5), should create 4 nodes with names A-D, and 5 directional
     * relationships between nodes in random way. And if there is any cycle path, return value should be true,
     * false otherwise. Cycle path means A->B->C->A for example.
     */
    @Procedure(value = "createNodesAndRelations", mode = Mode.WRITE)
    @Description("Create list of nodes and relations.")
    public Stream<Output> createNodesAndRelations(@Name("nodes") List<String> nodes, @Name("relations") long relations) {
        try (Transaction tx = db.beginTx()) {
            List<Node> createdNodes = new ArrayList<>();
            List<Long> nodeIds = new ArrayList<>();

            nodes.stream().forEach(s -> {
                Node node = db.createNode(Label.label("node"));
                node.setProperty("name", s);
                createdNodes.add(node);
                nodeIds.add(node.getId());
            });
            int size = createdNodes.size();
            if (size > 1) {
                for (int i = 0; i < relations; i++) {
                    int toIndex = 0;
                    int fromIndex = 0;
                    while (size > 1 && toIndex == fromIndex) {
                        toIndex = new Random().nextInt(size);
                        fromIndex = new Random().nextInt(size);
                    }
                    Node fromNode = createdNodes.get(fromIndex);
                    Node toNode = createdNodes.get(toIndex);
                    RelationshipType relation = RelationshipType.withName("RELATION");
                    fromNode.createRelationshipTo(toNode, relation);
                }
            }
            HashMap<String, Object> parameters = new HashMap<>();
            parameters.put("nodesids", nodeIds);
            Result result = db.execute(CYCLES_QUERY, parameters);
            tx.success();
            return result.stream().map(Output::new);
        }
    }

    public static class Output {
        public Boolean result = false;

        public Output(Map<String, Object> stringObjectMap) {
            result = (Boolean) stringObjectMap.get("result");
        }
    }
}
