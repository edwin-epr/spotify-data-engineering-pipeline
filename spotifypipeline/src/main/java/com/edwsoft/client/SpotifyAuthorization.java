package com.edwsoft.client;

import com.edwsoft.config.PipelineConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

public class SpotifyAuthorization {
    private static final Logger logger = LoggerFactory.getLogger(SpotifyAuthorization.class);
    private final PipelineConfig pipelineConfig;
    private final HttpClient httpClient;

    public SpotifyAuthorization(HttpClient httpClient, PipelineConfig pipelineConfig) {
        this.httpClient = httpClient;
        this.pipelineConfig = pipelineConfig;
    }

    public Optional<String> getAccessToken() {
        String clientId = pipelineConfig.getClientId();
        String clientSecret = pipelineConfig.getClientSecret();
        String tokenUrl = pipelineConfig.getTokenUrl();

        String credentials = String.format("%s:%s", clientId, clientSecret);
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

        String grantType = "grant_type=client_credentials";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenUrl))
                .header("Authorization", String.format("Basic %s", encodedCredentials))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(grantType))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                logger.error("Failed to retrieve data due to the status code: {}.", response.statusCode());
                return Optional.empty();
            }

            ObjectMapper mapper = new JsonMapper();
            JsonNode json = mapper.readTree(response.body());

            return Optional
                    .ofNullable(json.get("access_token"))
                    .map(JsonNode::asText)
                    .filter(token -> !token.isEmpty());

        } catch (IOException e) {
            logger.error("I/O error: {}.", e.getMessage(), e);
            return Optional.empty();
        } catch (InterruptedException e) {
            logger.error("Interruption error: {}.", e.getMessage(), e);
            return Optional.empty();
        }

    }
}
