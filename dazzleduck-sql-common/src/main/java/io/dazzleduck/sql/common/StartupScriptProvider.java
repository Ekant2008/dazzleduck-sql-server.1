package io.dazzleduck.sql.common;

import com.typesafe.config.Config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides the SQL script executed once on startup against the singleton DuckDB connection.
 *
 * <p>Load via {@link #load(Config)}, which honours a {@code startup_script_provider} block:
 * <pre>{@code
 * startup_script_provider {
 *     # class = "com.example.CustomProvider"   # optional; defaults to ConfigBasedStartupScriptProvider
 *     content = "INSTALL arrow FROM community; LOAD arrow;"
 *     # script_location = "/path/to/startup.sql"
 * }
 * }</pre>
 */
public interface StartupScriptProvider {

    String STARTUP_SCRIPT_CONFIG_PREFIX = "startup_script_provider";
    Pattern ENV_VAR_PATTERN = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}");

    void setConfig(Config config);

    String getStartupScript() throws IOException;

    /**
     * Loads a {@link StartupScriptProvider} from the given config, using the
     * {@code startup_script_provider} block. Falls back to {@link ConfigBasedStartupScriptProvider}
     * when the block is absent or has no {@code class} key.
     */
    static StartupScriptProvider load(Config config) throws Exception {
        ConfigBasedStartupScriptProvider defaultProvider = new ConfigBasedStartupScriptProvider();
        if (!config.hasPath(STARTUP_SCRIPT_CONFIG_PREFIX)) {
            return defaultProvider;
        }
        Config providerConfig = config.getConfig(STARTUP_SCRIPT_CONFIG_PREFIX);
        if (!providerConfig.hasPath("class")) {
            defaultProvider.setConfig(providerConfig);
            return defaultProvider;
        }
        String className = providerConfig.getString("class");
        try {
            var ctor = Class.forName(className).getConstructor(Config.class);
            return (StartupScriptProvider) ctor.newInstance(providerConfig);
        } catch (NoSuchMethodException e) {
            var provider = (StartupScriptProvider) Class.forName(className).getConstructor().newInstance();
            provider.setConfig(providerConfig);
            return provider;
        }
    }

    /**
     * Replaces {@code ${VAR_NAME}} references in {@code content} with values from
     * {@link System#getenv}. Throws {@link IllegalArgumentException} if any variable is missing.
     */
    static String replaceEnvVariable(String content) {
        Matcher matcher = ENV_VAR_PATTERN.matcher(content);
        StringBuilder result = new StringBuilder();
        List<String> missing = new ArrayList<>();
        while (matcher.find()) {
            String varName = matcher.group(1);
            String value = System.getenv(varName);
            if (value == null) {
                missing.add(varName);
            } else {
                matcher.appendReplacement(result, Matcher.quoteReplacement(value));
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "Startup script references undefined environment variable(s): " + missing);
        }
        matcher.appendTail(result);
        return result.toString();
    }
}
