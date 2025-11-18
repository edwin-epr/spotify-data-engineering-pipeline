package com.edwsoft.config;

import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

@Getter
public class PipelineConfig {

    private static final PipelineConfig INSTANCE = new PipelineConfig();
    private final Properties props = new Properties();

    private final String clientId;
    private final String clientSecret;
    private final String redirectUri;
    private final String baseUrl;
    private final String tokenUrl;

    private PipelineConfig() {
        loadProperties();
        clientId = loadFromEnvironmentVariables("SPOTIFY_CLIENT_ID");
        clientSecret = loadFromEnvironmentVariables("SPOTIFY_CLIENT_SECRET");
        redirectUri = loadFromProperties("spotify.redirect.uri");
        baseUrl = loadFromProperties("spotify.base.url");
        tokenUrl = loadFromProperties("spotify.token.url");
    }

    private void loadProperties() {
        try(InputStream input = PipelineConfig.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {

            if (input == null) {
                throw new IllegalStateException("Unable to load application.properties");
            }
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    public static PipelineConfig getInstance() {
        return INSTANCE;
    }

    private String loadFromEnvironmentVariables(String variable) {
        String value = System.getenv(variable);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException(
                    String.format("Environment variable '%s' is not set or empty", variable)
            );
        }
        return value;
    }

    private String loadFromProperties(String key) {
        String value = props.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException(
                    String.format("Environment variable '%s' is not set or empty", key)
            );
        }
        return value;
    }

    private String loadFromProperties(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }
}
