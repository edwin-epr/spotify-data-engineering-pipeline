package com.edwsoft.catalog;

import com.edwsoft.client.SpotifyApiClient;
import com.edwsoft.processor.SpotifyDataProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class SpotifyCatalogClient {
    private static final Logger logger = LoggerFactory.getLogger(SpotifyCatalogClient.class);
    private final SpotifyApiClient spotifyApiClient;
    private final SparkSession sparkSession;

    public SpotifyCatalogClient(SpotifyApiClient spotifyApiClient, SparkSession sparkSession) {
        this.spotifyApiClient = spotifyApiClient;
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> processPlaylist(String playlistId) {
        logger.info("Processing playlist {}", playlistId);
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
        items.forEach(itemObject -> {
            JsonNode item = itemObject.get("item");
            if(!Objects.isNull(item) && !item.isNull()) {
                tracks.add(item);
            }
        });

        logger.info("Extracted {} tracks from playlist: {}.", tracks.size(), playlistId);
        return SpotifyDataProcessor.jsonToDataFrame(tracks, sparkSession);
    }

    public Dataset<Row> getArtistsNameIds(String playlistsId, Dataset<Row> processedPlaylist) {
        logger.info("Getting artists names from playlist: {}.", playlistsId);
        Dataset<Row> artistsMatched = processedPlaylist.selectExpr(
                "inline(arrays_zip(artists.id, artists.name)) as (artist_id, artist_name)"
        ).select(
                "artist_id",
                "artist_name"
        );

        return artistsMatched;
    }

    public Dataset<Row> getPlaylistsMetadata(String playlistId, Dataset<Row> processedPlaylist) {
        logger.info("Getting playlists metadata from playlist: {}.", playlistId);
        Dataset<Row> playlists = processedPlaylist.select(
                functions.col("album.album_type"),
                functions.col("album.id").alias("album_id"),
                functions.col("album.name").alias("album_name"),
                functions.col("album.release_date"),
                functions.col("album.total_tracks"),
                functions.col("artists.name").getItem(0).alias("main_artist_name"),
                functions.col("duration_ms"),
                functions.col("id").alias("track_id"),
                functions.col("name").alias("track_name"),
                functions.col("popularity")
        );

        return playlists;
    }

    public Dataset<Row> getAlbums(List<String> albumsIds) {
        logger.info("Getting albums metadata from {} albums.", albumsIds.size());
        Dataset<Row> albums = spotifyApiClient.fetchBatchJson("albums", albumsIds)
                .select(
                        functions.col("name").alias("album_name"),
                        functions.col("release_date"),
                        functions.col("type"),
                        functions.col("artists.name").getItem(0).alias("artist_name"),
                        functions.explode(functions.col("tracks.items.id")).alias("track_id"),
                        functions.col("popularity")
                );
        return albums;
    }

}
