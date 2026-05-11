package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Parsed entry from the {@code access} JWT claim.
 *
 * <p>Wire format: {@code [type, name, projection, filter]}
 * <ul>
 *   <li><b>type</b>       — {@code "table"}, {@code "path"}, or {@code "function"}</li>
 *   <li><b>name</b>       — table name, path prefix, or function name</li>
 *   <li><b>projection</b> — {@code "*"} (column restriction reserved for future use)</li>
 *   <li><b>filter</b>     — compiled SQL WHERE expression; {@code "true"} means no restriction</li>
 * </ul>
 */
public record TableAccessEntry(String type, String name, JsonNode filter) {

    public static final String TABLE    = "table";
    public static final String PATH     = "path";
    public static final String FUNCTION = "function";
}
