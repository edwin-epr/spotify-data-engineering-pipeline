package com.edwsoft.config;

import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

@Getter
public class PipelineConfig {

    private final String clientId;
    private final String clientSecret;
    private final String redirectUri;
    private final String baseUrl;
    private final String tokenUrl;

    public PipelineConfig() {
        clientId = loadFromEnvironmentVariables("SPOTIFY_CLIENT_ID");
        clientSecret = loadFromEnvironmentVariables("SPOTIFY_CLIENT_SECRET");
        redirectUri = loadFromEnvironmentVariables("SPOTIFY_REDIRECT_URI");
        baseUrl = loadFromEnvironmentVariables("SPOTIFY_BASE_URL");
        tokenUrl = loadFromEnvironmentVariables("SPOTIFY_TOKEN_URL");
    }

    private String loadFromEnvironmentVariables(String variable) {
        return Optional.ofNullable(System.getenv(variable))
                .filter(str -> !str.trim().isEmpty())
                .map(String::trim)
                .orElseThrow(() -> new IllegalStateException(
                        String.format("Environment variable '%s' is not set or empty", variable)
                ));
    }
}
