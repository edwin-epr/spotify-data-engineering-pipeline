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
    private final String normalizedBaseURL;

    public SpotifyApiClient(HttpClient httpClient, SpotifyAuthorization spotifyAuthorization, SparkSession sparkSession,
                            PipelineConfig pipelineConfig) {
        this.httpClient = httpClient;
        this.spotifyAuthorization = spotifyAuthorization;
        this.sparkSession = sparkSession;
        String rawBaseURL = pipelineConfig.getBaseUrl();
        this.normalizedBaseURL = rawBaseURL.endsWith("/") ? rawBaseURL : rawBaseURL + "/";
    }

    public JsonNode fetchJson(String endpoint, String spotifyId, Map<String, String> params) {
        Objects.requireNonNull(endpoint, "Endpoint must not be null");

        URI uri = buildURI(endpoint, spotifyId, params);

        int maxRetries = PipelineConfig.PIPELINE_MAX_HTTP_RETRIES;
        int currentRetry = 0;
        while (currentRetry < maxRetries) {
            try {

                String token = spotifyAuthorization
                        .getAccessToken()
                        .orElseThrow(() -> new RuntimeException("Failed to get access token"));

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .header("Authorization", String.format("Bearer %s", token))
                        .GET()
                        .build();

                HttpResponse<String> response = httpClient
                        .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

                if (response.statusCode() == 429) {
                    long retryAfter = response.headers().firstValue("Retry-After")
                            .map(Long::parseLong)
                            .orElse(PipelineConfig.PIPELINE_DEFAULT_RETRY_AFTER_SEC);

                    logger.warn("Rate limit hit (429) for {}/{}. Waiting {} seconds before retry {}/{}...", endpoint, spotifyId, retryAfter, currentRetry + 1, maxRetries);
                    Thread.sleep(retryAfter * 1000);
                    currentRetry++;
                    continue;
                }
                if (response.statusCode() < 200 || response.statusCode() > 299) {
                    throw new HttpException(response.statusCode(), response.body());
                }

                return MAPPER.readTree(response.body());

            } catch (IOException e) {
                logger.error("I/O Error: {}", e.getMessage(), e);
                return createEmptyJsonObject();
            } catch (InterruptedException e) {
                logger.error("Thread interrupted during retry for {}/{}.", endpoint, spotifyId, e);
                Thread.currentThread().interrupt();
                return createEmptyJsonObject();
            }
        }
       logger.error("Max retries ({}) reached for {}/{}. Skipping.", maxRetries, endpoint, spotifyId);
        return createEmptyJsonObject();
    }

    public JsonNode fetchJson(String endpoint, String spotifyId) {
        return fetchJson(endpoint, spotifyId, Map.of());
    }

    public JsonNode fetchJson(String endpoint) {
        return fetchJson(endpoint, null, Map.of());
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

    private URI addParamsToURI(StringBuilder uriBuilder, Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return URI.create(uriBuilder.toString());
        }

        uriBuilder.append("?");

        boolean isFirstElement = true;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (!isFirstElement) {
                uriBuilder.append("&");
            }
            uriBuilder.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8))
                    .append("=")
                    .append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
            isFirstElement = false;
        }

        return URI.create(uriBuilder.toString());
    }

    private URI buildURI(String endpoint, String spotifyId, Map<String, String> params) {

        String normalizedEndpoint = endpoint.startsWith("/") ? endpoint.substring(1) : endpoint;

        StringBuilder uriBuilder = new StringBuilder(normalizedBaseURL).append(normalizedEndpoint);

        if (spotifyId != null && !spotifyId.isEmpty()) {
            uriBuilder.append("/").append(spotifyId);
        }

        URI uri = addParamsToURI(uriBuilder, params);

        logger.debug("Built URI: {}", uri);

        return uri;

    }

    private JsonNode createEmptyJsonObject() {
        return MAPPER.createObjectNode();
    }
}
