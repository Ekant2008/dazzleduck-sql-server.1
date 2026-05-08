package io.dazzleduck.sql.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.authorization.SqlAuthorizer;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TransformationsInjectFilterTest {

    private static DuckDBConnection conn;

    @BeforeAll
    static void setup() throws SQLException {
        conn = ConnectionPool.getConnection();
        conn.createStatement().execute("CREATE TABLE orders (id INT, tenant_id VARCHAR, amount INT)");
        conn.createStatement().execute("INSERT INTO orders VALUES (1,'abc',100),(2,'xyz',200),(3,'abc',300)");
        conn.createStatement().execute("CREATE TABLE items (order_id INT, tenant_id VARCHAR, name VARCHAR)");
        conn.createStatement().execute("INSERT INTO items VALUES (1,'abc','widget'),(2,'xyz','gadget'),(3,'abc','thing')");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.createStatement().execute("DROP TABLE IF EXISTS orders");
        conn.createStatement().execute("DROP TABLE IF EXISTS items");
        conn.close();
    }

    private JsonNode filter(String expr) {
        return SqlAuthorizer.compileFilterString(expr);
    }

    private List<Object> execFirstColumn(String sql) throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery(sql);
        List<Object> rows = new ArrayList<>();
        while (rs.next()) rows.add(rs.getObject(1));
        return rows;
    }

    // --- single table ---

    @Test
    void singleTable_filterInjected() throws SQLException, JsonProcessingException {
        String sql = "SELECT id FROM orders";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"), "CTE name should appear in output SQL");

        List<Object> ids = execFirstColumn(out);
        assertEquals(List.of(1, 3), ids);
    }

    @Test
    void singleTable_noFilterClause_cteMapEmpty_whenNoTables() throws SQLException, JsonProcessingException {
        // Constant query — no base tables — cte_map should remain empty
        String sql = "SELECT 1 as x";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        JsonNode cteMap = result.get("statements").get(0).get("node").get("cte_map").get("map");
        assertEquals(0, cteMap.size());
    }

    // --- JOIN ---

    @Test
    void join_filterInjectedIntoEachArm() throws SQLException, JsonProcessingException {
        String sql = "SELECT o.id FROM orders o JOIN items i ON o.id = i.order_id";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___orders"), "orders CTE should be injected");
        assertTrue(out.contains("___items"), "items CTE should be injected");

        // Both tables filtered to tenant 'abc': ids 1 and 3
        List<Object> ids = execFirstColumn(out);
        assertEquals(2, ids.size());
        assertTrue(ids.contains(1));
        assertTrue(ids.contains(3));
    }

    @Test
    void join_cteMapHasTwoEntries() throws SQLException, JsonProcessingException {
        String sql = "SELECT * FROM orders JOIN items ON orders.id = items.order_id";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        JsonNode cteMap = result.get("statements").get(0).get("node").get("cte_map").get("map");
        assertEquals(2, cteMap.size());
    }

    // --- schema-qualified table ---

    @Test
    void schemaQualified_cteKeyIncludesSchema() throws SQLException, JsonProcessingException {
        String sql = "SELECT id FROM main.orders";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        JsonNode cteMap = result.get("statements").get(0).get("node").get("cte_map").get("map");
        assertEquals(1, cteMap.size());
        String cteKey = cteMap.get(0).get("key").asText();
        assertTrue(cteKey.startsWith("___main_"), "CTE key should include schema: " + cteKey);

        String out = Transformations.parseToSql(conn, result);
        List<Object> ids = execFirstColumn(out);
        assertEquals(List.of(1, 3), ids);
    }

    // --- subquery in FROM ---

    @Test
    void subqueryInFrom_filterInjectedIntoSubquery() throws SQLException, JsonProcessingException {
        String sql = "SELECT id FROM (SELECT id, tenant_id FROM orders) sub";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        List<Object> ids = execFirstColumn(out);
        assertEquals(List.of(1, 3), ids);
    }

    // --- user CTE referencing a base table ---

    @Test
    void userCte_filterInjectedIntoBody() throws SQLException, JsonProcessingException {
        String sql = "WITH user_cte AS (SELECT id, tenant_id FROM orders) SELECT id FROM user_cte";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        // The filter CTE for orders should precede user_cte
        JsonNode cteMap = result.get("statements").get(0).get("node").get("cte_map").get("map");
        assertTrue(cteMap.size() >= 2);
        // First entry should be the injected filter CTE for orders
        String firstKey = cteMap.get(0).get("key").asText();
        assertTrue(firstKey.startsWith("___"), "Filter CTE should be first: " + firstKey);

        List<Object> ids = execFirstColumn(out);
        assertEquals(List.of(1, 3), ids);
    }

    // --- same table referenced twice (self-join) ---

    @Test
    void selfJoin_onlyOneCteCreated() throws SQLException, JsonProcessingException {
        String sql = "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        JsonNode cteMap = result.get("statements").get(0).get("node").get("cte_map").get("map");
        // Only one CTE should be created for 'orders' even though it appears twice
        assertEquals(1, cteMap.size());

        String out = Transformations.parseToSql(conn, result);
        List<Object> ids = execFirstColumn(out);
        assertEquals(2, ids.size());
    }

    // --- WHERE subquery (IN, EXISTS, scalar) — security gap fixed ---

    @Test
    void whereInSubquery_innerTableFiltered() throws SQLException, JsonProcessingException {
        // items in the IN subquery must get the filter — otherwise tenant xyz rows leak
        String sql = "SELECT id FROM orders WHERE id IN (SELECT order_id FROM items)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___items"), "items must get a filter CTE");
        assertTrue(out.contains("___orders"), "orders must get a filter CTE");

        List<Object> ids = execFirstColumn(out);
        // orders filtered to {1,3}; items filtered to {1,3}; intersection = {1,3}
        assertEquals(List.of(1, 3), ids);
    }

    @Test
    void whereNotInSubquery_innerTableFiltered() throws SQLException, JsonProcessingException {
        String sql = "SELECT id FROM orders WHERE id NOT IN (SELECT order_id FROM items WHERE name = 'widget')";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___items"), "items must get a filter CTE");
        List<Object> ids = execFirstColumn(out);
        // orders {1,3}, items filtered to {1} (widget+abc); NOT IN removes id=1 → {3}
        assertEquals(List.of(3), ids);
    }

    @Test
    void whereExistsSubquery_innerTableFiltered() throws SQLException, JsonProcessingException {
        // Correlated EXISTS — items in the subquery must be filtered
        String sql = "SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___items"), "items must get a filter CTE");
        assertTrue(out.contains("___orders"), "orders must get a filter CTE");

        List<Object> ids = execFirstColumn(out);
        // orders {1,3} both have matching items in tenant abc
        assertEquals(2, ids.size());
        assertTrue(ids.contains(1));
        assertTrue(ids.contains(3));
    }

    @Test
    void scalarSubqueryInSelectList_innerTableFiltered() throws SQLException, JsonProcessingException {
        String sql = "SELECT id, (SELECT count(*) FROM items WHERE items.order_id = orders.id) AS cnt FROM orders";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));
        String out = Transformations.parseToSql(conn, result);

        assertTrue(out.contains("___items"), "items in scalar subquery must get a filter CTE");
        assertTrue(out.contains("___orders"), "orders must get a filter CTE");

        // orders for tenant abc: ids 1 and 3; each has one matching item in tenant abc
        List<Object> cnts = new ArrayList<>();
        var rs = conn.createStatement().executeQuery(out);
        while (rs.next()) cnts.add(rs.getObject(2)); // cnt column
        assertEquals(List.of(1L, 1L), cnts);
    }

    // --- UNION ---

    @Test
    void union_filterInjectedIntoBothSides() throws SQLException, JsonProcessingException {
        // Both arms reference the same table; after filter each arm returns 2 rows (tenant 'abc')
        // UNION ALL → 4 rows total (no dedup)
        String sql = "SELECT id FROM orders WHERE amount > 0 UNION ALL SELECT id FROM orders WHERE amount < 1000";
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode result = Transformations.injectFilterCtes(tree, filter("tenant_id = 'abc'"));

        // Top-level is SET_OPERATION_NODE; cte_map is on it
        JsonNode cteMap = result.get("statements").get(0).get("node").get("cte_map").get("map");
        assertEquals(1, cteMap.size(), "One filter CTE for orders");

        String out = Transformations.parseToSql(conn, result);
        List<Object> ids = execFirstColumn(out);
        assertEquals(4, ids.size(), "UNION ALL: 2 rows × 2 arms");
    }
}
