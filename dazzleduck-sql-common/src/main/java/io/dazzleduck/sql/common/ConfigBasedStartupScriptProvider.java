package io.dazzleduck.sql.common;

import com.typesafe.config.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Default {@link StartupScriptProvider} that reads SQL from inline {@code content} and/or a file
 * at {@code script_location}, with {@code ${ENV_VAR}} substitution applied to file content.
 */
public class ConfigBasedStartupScriptProvider implements StartupScriptProvider {

    private static final String CONTENT_KEY         = "content";
    public  static final String SCRIPT_LOCATION_KEY = "script_location";

    private Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public String getStartupScript() throws IOException {
        StringBuilder sb = new StringBuilder();
        if (config != null && config.hasPath(CONTENT_KEY)) {
            sb.append(config.getString(CONTENT_KEY)).append("\n");
        }
        String location = (config != null && config.hasPathOrNull(SCRIPT_LOCATION_KEY))
                ? config.getString(SCRIPT_LOCATION_KEY) : null;
        if (location != null && !location.isBlank()) {
            Path path = Paths.get(location);
            if (Files.isRegularFile(path)) {
                sb.append(StartupScriptProvider.replaceEnvVariable(Files.readString(path).trim()))
                  .append("\n");
            }
        }
        return sb.toString();
    }
}
