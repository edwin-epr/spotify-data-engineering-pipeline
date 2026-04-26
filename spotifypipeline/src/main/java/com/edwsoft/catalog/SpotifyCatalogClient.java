package com.edwsoft.catalog;

import com.edwsoft.client.SpotifyApiClient;
import com.edwsoft.processor.SpotifyDataProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Objects;

public class SpotifyCatalogClient {
    private final SpotifyApiClient spotifyApiClient;
    private final SparkSession sparkSession;

    public SpotifyCatalogClient(SpotifyApiClient spotifyApiClient, SparkSession sparkSession) {
        this.spotifyApiClient = spotifyApiClient;
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> processPlaylist(String playlistId) {
        String endpoint = String.format("playlists/%s/items", playlistId);
        JsonNode playlist = spotifyApiClient.fetchJson(endpoint);
        JsonNode items =  playlist.get("items");

        Objects.requireNonNull(items, "items is null.");
        if (items.isNull()) {
            throw new RuntimeException("items contains JSON null value.");
        }
        if (items.isEmpty()) {
            throw new RuntimeException("items is empty.");
        }

        JsonMapper mapper = new JsonMapper();
        ArrayNode tracks = mapper.createArrayNode();
        items.forEach(item -> {
            JsonNode track = item.get("track");
            if(!Objects.isNull(track) && !track.isNull()) {
                tracks.add(track);
            }
        });

        return SpotifyDataProcessor.jsonToDataFrame(tracks, sparkSession);
    }


}
