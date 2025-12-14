package com.edwsoft.client;

import com.edwsoft.config.PipelineConfig;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

public class SpotifyAuthorization {

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
                System.out.printf("Failed to retrieve data due to the status code: %d.%n",
                        response.statusCode());
                return Optional.empty();
            }

            ObjectMapper mapper = new JsonMapper();
            JsonNode json = mapper.readTree(response.body());

            return Optional
                    .ofNullable(json.get("access_token"))
                    .map(JsonNode::asString)
                    .filter(token -> !token.isEmpty());

        } catch (IOException e) {
            System.out.printf("I/O error: %s%n", e.getMessage());
            return Optional.empty();
        } catch (InterruptedException e) {
            System.out.printf("Interruption error: %s%n", e.getMessage());
            return Optional.empty();
        }

    }
}
