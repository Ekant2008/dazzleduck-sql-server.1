package io.dazzleduck.sql.flight;

/**
 * @deprecated Use {@link io.dazzleduck.sql.common.ConfigBasedStartupScriptProvider}.
 *             This class is retained for backward compatibility with configurations that reference
 *             {@code class = "io.dazzleduck.sql.flight.ConfigBasedStartupScriptProvider"}.
 */
@Deprecated
public class ConfigBasedStartupScriptProvider
        extends io.dazzleduck.sql.common.ConfigBasedStartupScriptProvider
        implements StartupScriptProvider {
}
