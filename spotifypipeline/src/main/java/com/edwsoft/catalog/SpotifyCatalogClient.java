package com.edwsoft.catalog;

import com.edwsoft.client.SpotifyApiClient;
import com.edwsoft.processor.SpotifyDataProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.spark.sql.*;
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
        logger.info("Processing playlist id: {}.", playlistId);
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

        logger.info("Extracted {} tracks from playlist id: {}.", tracks.size(), playlistId);
        return SpotifyDataProcessor.jsonToDataFrame(tracks, sparkSession);
    }

    public Dataset<Row> getArtistsNameIds(String playlistsId, Dataset<Row> processedPlaylist) {
        logger.info("Getting artists names from playlist id: {}.", playlistsId);
        Dataset<Row> artistsMatched = processedPlaylist.selectExpr(
                "inline(arrays_zip(artists.id, artists.name)) as (artist_id, artist_name)"
        ).select(
                "artist_id",
                "artist_name"
        );

        return artistsMatched;
    }

    public Dataset<Row> getPlaylistsMetadata(String playlistId, Dataset<Row> processedPlaylist) {
        logger.info("Getting playlists metadata from playlist id: {}.", playlistId);
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

    public Dataset<Row> getAlbumTracks(List<String> albumsIds) {
        logger.info("Getting tracks from {} albums.", albumsIds.size());
        Dataset<Row> albums = spotifyApiClient.fetchBatchJson("albums", albumsIds)
                .select(
                        functions.col("id").alias("album_id"),
                        functions.col("name").alias("album_name"),
                        functions.col("release_date"),
                        functions.col("type"),
                        functions.col("artists.name").getItem(0).alias("artist_name"),
                        functions.explode(functions.col("tracks.items.id")).alias("track_id"),
                        functions.col("popularity")
                );

        logger.debug("Returning {} tracks from {} albums.", albums.count(), albumsIds.size());

        return albums;
    }

    public Dataset<Row> getDistinctTracksFromPlaylistAlbums(String playlistsIds, Dataset<Row> processedPlaylist) {
        logger.info("Getting album tracks from playlist id: {}.", playlistsIds);

        List<String> albumsIds = getPlaylistsMetadata(playlistsIds, processedPlaylist)
                .select("album_id")
                .as(Encoders.STRING())
                .collectAsList();

        logger.info("Found {} albums in playlist id: {}.", albumsIds.size(), playlistsIds);

        Dataset<Row> albums = getAlbumTracks(albumsIds).distinct();

        logger.info("Returning {} distinct tracks from playlist id: {}.", albums.count(), playlistsIds);

        return albums;
    }

    public List<String> getTracksList(String playlistId, Dataset<Row> albumTracks) {
        logger.info("Getting tracks from playlist id: {}.", playlistId);

        List<String> tracksList = albumTracks
                .select("track_id")
                .as(Encoders.STRING())
                .collectAsList();

        logger.info("Found {} tracks in playlist id: {}.", tracksList.size(), playlistId);
        return tracksList;
    }

    public Dataset<Row> getTracks(List<String> tracksList) {
       logger.info("Getting {} tracks.", tracksList.size());

       Dataset<Row> tracks = spotifyApiClient.fetchBatchJson("tracks", tracksList);

       logger.debug("Returning {} tracks successfully.", tracks.count());

       return tracks;
    }

    public Dataset<Row> getAlbumsTracksBasedOnPlaylist(String playlistId, Dataset<Row> albumTracks, List<String> tracksList) {
        logger.info("Getting albums tracks based on playlist id: {}.", playlistId);

        Dataset<Row> tracks = getTracks(tracksList);

        Dataset<Row> albumsTracksJoined = albumTracks.join(
                tracks,
                albumTracks.col("track_id").equalTo(tracks.col("id")),
                "inner"
        ).drop(albumTracks.col("popularity"));

        Dataset<Row> albumsTracksJoinedCompleted = albumsTracksJoined.select(
                functions.col("album_name"),
                functions.col("release_date"),
                functions.col("artist_name"),
                functions.col("track_id"),
                functions.col("name").alias("track_name"),
                functions.col("duration_ms").divide(1000).alias("duration_seconds"),
                functions.col("popularity"),
                functions.col("track_number")
        );
        logger.info("Album tracks join completed for playlist id: {}.", playlistId);
        logger.debug("Returning {} joined album tracks from playlist id: {}.", albumsTracksJoinedCompleted.count(), playlistId);
        return albumsTracksJoinedCompleted;

    }

}
