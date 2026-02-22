package com.edwsoft.catalog;

import com.edwsoft.client.SpotifyApiClient;
import com.edwsoft.processor.SpotifyDataProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SpotifyCatalogClient {
    private final SpotifyApiClient spotifyApiClient;
    private final SparkSession sparkSession;

    public SpotifyCatalogClient(SpotifyApiClient spotifyApiClient, SparkSession sparkSession) {
        this.spotifyApiClient = spotifyApiClient;
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> processPlaylist(String playlistId) {
        String endpoint = "playlists";
        String objectId = String.format("%s/tracks", playlistId);
        JsonNode playlist = spotifyApiClient.fetchJson(endpoint, playlistId);
        JsonNode items =  playlist.get("tracks").get("items");

        return SpotifyDataProcessor.jsonToDataFrame(items, sparkSession);
    }
}
