package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.Headers;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Authorizer for RESTRICT_READ_ONLY mode: SELECT-only access with a mandatory filter
 * injected into every base table via CTEs.
 *
 * <p>Two filter claim formats are supported in the JWT:
 * <ul>
 *   <li><b>{@code access}</b> — a JSON array of 3-element tuples
 *       {@code [[tableName, projection, filter], ...]}. All three elements are required.
 *       Use {@code "*"} for all columns and {@code "true"} for no row filter.
 *       Column projection other than {@code "*"} is reserved for future implementation.
 *       Takes precedence over {@code filter} when present.</li>
 *   <li><b>{@code filter}</b> — a single SQL expression applied to every table
 *       (e.g. {@code tenant_id = 'abc'}). All tables must expose the filter column.</li>
 * </ul>
 * At least one claim must be present; otherwise the query is rejected.
 */
public class RestrictedReadOnlyAuthorizer implements SqlAuthorizer {

    public static final SqlAuthorizer INSTANCE = new RestrictedReadOnlyAuthorizer();

    private RestrictedReadOnlyAuthorizer() {}

    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query,
                              Map<String, String> verifiedClaims) throws UnauthorizedException {
        SelectOnlyAuthorizer.INSTANCE.authorize(user, database, schema, query, verifiedClaims);

        String tableAccessStr = verifiedClaims.get(Headers.HEADER_ACCESS);
        if (tableAccessStr != null && !tableAccessStr.isBlank()) {
            var entries = SqlAuthorizer.parseTableAccess(tableAccessStr);
            Map<String, JsonNode> tableFilters = new LinkedHashMap<>();
            for (var entry : entries) {
                if (!TableAccessEntry.TABLE.equals(entry.type())) {
                    throw new UnauthorizedException(
                            "RESTRICT_READ_ONLY mode only supports type 'table' in access; got: '" + entry.type() + "'");
                }
                tableFilters.put(entry.name(), entry.filter());
            }
            return SqlAuthorizer.addFilterViaCtes(query, tableFilters);
        }

        String filterStr = verifiedClaims.get(Headers.HEADER_FILTER);
        if (filterStr == null || filterStr.isBlank()) {
            throw new UnauthorizedException(
                    "RESTRICT_READ_ONLY mode requires a 'access' or 'filter' claim in the JWT");
        }
        String tableStr = verifiedClaims.get(Headers.HEADER_TABLE);
        if (tableStr == null || tableStr.isBlank()) {
            throw new UnauthorizedException(
                    "RESTRICT_READ_ONLY mode: 'filter' claim requires a 'table' claim; " +
                    "use 'access' for multi-table queries");
        }
        // filter + table is shorthand for a single-entry access: [[table, "*", filter]]
        Map<String, JsonNode> tableFilters = new LinkedHashMap<>();
        tableFilters.put(tableStr, SqlAuthorizer.compileFilterString(filterStr));
        return SqlAuthorizer.addFilterViaCtes(query, tableFilters);
    }

    @Override
    public boolean hasWriteAccess(String user, String ingestionQueue, Map<String, String> verifiedClaims) {
        return false;
    }
}
