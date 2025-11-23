package com.edwsoft.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(SystemStubsExtension.class)
public class PipelineConfigTest {

    @SystemStub
    private final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test
    public void testConstructorLoadsEnvironmentVariablesTest() {

        environmentVariables
                .set("SPOTIFY_CLIENT_ID", "test-client-id")
                .set("SPOTIFY_CLIENT_SECRET", "test-client-secret")
                .set("SPOTIFY_REDIRECT_URI", "http://127.0.0.1:8888/spotify/api/callback")
                .set("SPOTIFY_BASE_URL", "https://api.spotify.com/v1")
                .set("SPOTIFY_TOKEN_URL", "https://accounts.spotify.com/api/token");

        PipelineConfig config = new PipelineConfig();

        assertEquals("test-client-id", config.getClientId());
        assertEquals("test-client-secret", config.getClientSecret());
        assertEquals("http://127.0.0.1:8888/spotify/api/callback", config.getRedirectUri());
        assertEquals("https://api.spotify.com/v1", config.getBaseUrl());
        assertEquals("https://accounts.spotify.com/api/token", config.getTokenUrl());
    }

    @Test
    public void testConstructorMissEnvironmentVariableTest() {

        environmentVariables
                .set("SPOTIFY_CLIENT_ID", "test-client-id")
                .set("SPOTIFY_REDIRECT_URI", "http://127.0.0.1:8888/spotify/api/callback")
                .set("SPOTIFY_BASE_URL", "https://api.spotify.com/v1")
                .set("SPOTIFY_TOKEN_URL", "https://accounts.spotify.com/api/token");

        IllegalStateException exception = assertThrows(IllegalStateException.class, PipelineConfig::new);

        assertEquals(
                "Environment variable 'SPOTIFY_CLIENT_SECRET' is not set or empty",
                exception.getMessage());
    }

    @Test
    public void testConstructorEmptyEnvironmentVariableTest() {

        environmentVariables
                .set("SPOTIFY_CLIENT_ID", "test-client-id")
                .set("SPOTIFY_CLIENT_SECRET", "test-client-secret")
                .set("SPOTIFY_REDIRECT_URI", " ")
                .set("SPOTIFY_BASE_URL", "https://api.spotify.com/v1")
                .set("SPOTIFY_TOKEN_URL", "https://accounts.spotify.com/api/token");

        IllegalStateException exception = assertThrows(IllegalStateException.class, PipelineConfig::new);

        assertEquals(
                "Environment variable 'SPOTIFY_REDIRECT_URI' is not set or empty",
                exception.getMessage());
    }
}
