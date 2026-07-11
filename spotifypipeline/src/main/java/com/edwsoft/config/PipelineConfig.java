package com.edwsoft.config;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

@Getter
public class PipelineConfig {
    private static final Logger logger  = LoggerFactory.getLogger(PipelineConfig.class);
    private final String clientId;
    private final String clientSecret;
    private final String redirectUri;
    private final String baseUrl;
    private final String tokenUrl;
    public static final int PIPELINE_MAX_HTTP_RETRIES=3;
    public static final long PIPELINE_DEFAULT_RETRY_AFTER_SEC=3L;

    public PipelineConfig() {
        clientId = loadFromEnvironmentVariables("SPOTIFY_CLIENT_ID");
        clientSecret = loadFromEnvironmentVariables("SPOTIFY_CLIENT_SECRET");
        redirectUri = loadFromEnvironmentVariables("SPOTIFY_REDIRECT_URI");
        baseUrl = loadFromEnvironmentVariables("SPOTIFY_BASE_URL");
        tokenUrl = loadFromEnvironmentVariables("SPOTIFY_TOKEN_URL");
        logger.info("Pipeline configuration loaded successfully.");
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
