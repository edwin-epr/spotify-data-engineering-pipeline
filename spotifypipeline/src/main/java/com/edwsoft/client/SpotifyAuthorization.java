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
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;

public class SpotifyAuthorization {
    private static final Logger logger = LoggerFactory.getLogger(SpotifyAuthorization.class);
    private static final ObjectMapper MAPPER = new JsonMapper();
    private final PipelineConfig pipelineConfig;
    private final HttpClient httpClient;
    private Token accessToken = new Token("", Instant.MIN);

    public SpotifyAuthorization(HttpClient httpClient, PipelineConfig pipelineConfig) {
        this.httpClient = httpClient;
        this.pipelineConfig = pipelineConfig;
    }

    public JsonNode fetchToken() {
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
                return MAPPER.createObjectNode();
            }

            JsonNode json = MAPPER.readTree(response.body());
            logger.debug("Successfully retrieved data due to the status code: {}.", response.statusCode());
            return json;

        } catch (IOException e) {
            logger.error("I/O error: {}.", e.getMessage(), e);
            return MAPPER.createObjectNode();
        } catch (InterruptedException e) {
            logger.error("Interruption error: {}.", e.getMessage(), e);
            return MAPPER.createObjectNode();
        }
    }

    public Optional<String> getAccessToken() {
        if (accessToken.isValid()) {
            logger.debug("Returning cached token, expires at: {}", accessToken.expiresAt());
            return Optional.ofNullable(accessToken.token());
        }

        logger.info("Fetching new access token.");
        JsonNode json = fetchToken();

        String token = Optional.ofNullable(json.get("access_token"))
                .map(JsonNode::asText)
                .filter(strToken -> !strToken.isEmpty())
                .orElse("");

        int expiresIn = Optional.ofNullable(json.get("expires_in"))
                .map(JsonNode::asInt)
                .orElse(0);

        if (token.isEmpty()) {
            logger.error("Access token not found in response.");
            return Optional.empty();
        }

        accessToken = new Token(token, Instant.now().plusSeconds(expiresIn));
        logger.info("New access token obtained, expires at: {}", accessToken.expiresAt());

        return Optional.ofNullable(accessToken.token())
                .filter(strObject -> !strObject.isEmpty());
    }
}
