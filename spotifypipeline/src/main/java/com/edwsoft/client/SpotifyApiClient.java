package com.edwsoft.client;

import com.edwsoft.exceptions.HttpException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SpotifyApiClient {

    private final HttpClient httpClient;
    private final SpotifyAuthorization spotifyAuthorization;
    private final SparkSession sparkSession;
    private final String baseUrl = System.getenv("SPOTIFY_BASE_URL");

    public SpotifyApiClient(HttpClient httpClient, SpotifyAuthorization spotifyAuthorization, SparkSession sparkSession) {
        this.httpClient = httpClient;
        this.spotifyAuthorization = spotifyAuthorization;
        this.sparkSession = sparkSession;
    }

    public JsonNode fetchJson(String endpoint, String spotifyId, Map<String, String> params) {

        Objects.requireNonNull(endpoint, "Endpoint must not be null");
        String buildUri = String.format("%s/%s", baseUrl, endpoint);

        if (spotifyId != null && !spotifyId.isEmpty()) {
            buildUri = String.format("%s/%s", buildUri, spotifyId);
        }

        URI uri = buildUriWithParams(buildUri, params);

        String token = spotifyAuthorization
                .getAccessToken()
                .orElseThrow(() -> new RuntimeException("Failed to get access token"));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .header("Authorization", String.format("Bearer %s", token))
                .GET()
                .build();

        try {
            HttpResponse<String> response = httpClient
                    .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            if (response.statusCode() < 200 || response.statusCode() > 299) {
                throw new HttpException(response.statusCode(), response.body());
            }

            JsonMapper mapper = new JsonMapper();
            return mapper.readTree(response.body());

        } catch (IOException e) {
            System.out.printf("I/O Error: %s%n", e.getMessage());
            return createEmptyJsonObject();
        } catch (InterruptedException e) {
            System.out.printf("Interrupted Error: %s%n", e.getMessage());
            return createEmptyJsonObject();
        }
    }

    public JsonNode fetchJson(String endpoint, String spotifyId) {
        return fetchJson(endpoint, spotifyId, new HashMap<>() {
        });
    }

    public JsonNode fetchJson(String endpoint) {
        return fetchJson(endpoint, null, new HashMap<>() {
        });
    }

    public Dataset<Row> fetchBatchJson(String endpoint, List<String> spotifyIds) {
        List<String> items = spotifyIds.stream()
                .map(spotifyId -> fetchJson(endpoint, spotifyId))
                .filter(jsonNode -> !jsonNode.isEmpty())
                .map(JsonNode::toString)
                .toList();

        Dataset<String> stringDataset = sparkSession.createDataset(items, Encoders.STRING());
        return sparkSession.read().json(stringDataset);
    }

    private URI buildUriWithParams(String uri, Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return URI.create(uri);
        }

        StringBuilder stringBuilder = new StringBuilder(uri);
        stringBuilder.setLength(stringBuilder.length() - 1); // Remove: /

        stringBuilder.append("?");
        params.forEach((key, value) -> stringBuilder
                .append(URLEncoder.encode(key, StandardCharsets.UTF_8))
                .append("=")
                .append(URLEncoder.encode(value, StandardCharsets.UTF_8))
                .append("&"));

        stringBuilder.setLength(stringBuilder.length() - 1); // Remove: &

        return URI.create(stringBuilder.toString());
    }

    private JsonNode createEmptyJsonObject() {
        JsonMapper mapper = new JsonMapper();
        return mapper.createObjectNode();
    }
}
