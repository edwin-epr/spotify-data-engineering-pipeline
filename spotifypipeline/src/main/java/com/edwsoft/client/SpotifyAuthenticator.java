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

public class SpotifyAuthenticator {

    private final PipelineConfig pipelineConfig = PipelineConfig.getInstance();
    private final HttpClient httpClient;
    private final String clientId = pipelineConfig.getClientId();
    private final String clientSecret = pipelineConfig.getClientSecret();
    private final String tokenUrl = pipelineConfig.getTokenUrl();

    public SpotifyAuthenticator(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public Optional<String> getAccessToken() {

        String auth = String.format("%s:%s", clientId, clientSecret);
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));

        String body = "grant_type=client_credentials";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenUrl))
                .header("Authorization", String.format("Basic %s", encodedAuth))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
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
