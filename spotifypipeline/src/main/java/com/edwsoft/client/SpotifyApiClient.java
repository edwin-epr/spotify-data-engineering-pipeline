package com.edwsoft.client;

import com.edwsoft.config.PipelineConfig;
import com.edwsoft.exceptions.HttpException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SpotifyApiClient {
    private static final Logger logger = LoggerFactory.getLogger(SpotifyApiClient.class);
    private static final JsonMapper MAPPER = new JsonMapper();
    private final HttpClient httpClient;
    private final SpotifyAuthorization spotifyAuthorization;
    private final SparkSession sparkSession;
    private final PipelineConfig pipelineConfig;

    public SpotifyApiClient(HttpClient httpClient, SpotifyAuthorization spotifyAuthorization, SparkSession sparkSession,
                            PipelineConfig pipelineConfig) {
        this.httpClient = httpClient;
        this.spotifyAuthorization = spotifyAuthorization;
        this.sparkSession = sparkSession;
        this.pipelineConfig = pipelineConfig;
    }

    public JsonNode fetchJson(String endpoint, String spotifyId, Map<String, String> params) {

        Objects.requireNonNull(endpoint, "Endpoint must not be null");
        String baseUrl = pipelineConfig.getBaseUrl();
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

            return MAPPER.readTree(response.body());

        } catch (IOException e) {
            logger.error("I/O Error: {}", e.getMessage(), e);
            return createEmptyJsonObject();
        } catch (InterruptedException e) {
            logger.error("Interrupted Error: {}", e.getMessage(), e);
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
                .map(spotifyId -> {
                    try {
                        return fetchJson(endpoint, spotifyId);
                    } catch (HttpException e) {
                        logger.error("Failed to fetch {}/{}: {}", endpoint, spotifyId, e.getMessage(), e);
                        return createEmptyJsonObject();
                    }
                })
                .filter(jsonNode -> !jsonNode.isEmpty())
                .map(JsonNode::toString)
                .toList();
        
        if (items.isEmpty()) {
            return sparkSession.emptyDataFrame();
        }

        Dataset<String> stringDataset = sparkSession.createDataset(items, Encoders.STRING());
        Dataset<Row> dataset = sparkSession.read().json(stringDataset);
        logger.info("Fetched {}/{} items successfully from endpoint: {}.", items.size(), spotifyIds.size(), endpoint);
        return dataset;
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
        return MAPPER.createObjectNode();
    }
}
