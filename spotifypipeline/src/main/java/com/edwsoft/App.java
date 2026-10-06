package com.edwsoft;

import com.edwsoft.catalog.SpotifyAnalytics;
import com.edwsoft.catalog.SpotifyCatalogClient;
import com.edwsoft.client.SpotifyApiClient;
import com.edwsoft.client.SpotifyAuthorization;
import com.edwsoft.config.PipelineConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.http.HttpClient;
import java.util.List;

/**
 * Spotify Data Engineering Project!
 *
 */
public class App 
{
    public static final int PIPELINE_MAX_HTTP_RETRIES = 3;
    public static final long PIPELINE_DEFAULT_RETRY_AFTER_SECONDS = 3L;

    public static void main( String[] args )
    {
        try(HttpClient client = HttpClient.newHttpClient()) {
            PipelineConfig pipelineConfig = new PipelineConfig(App.PIPELINE_MAX_HTTP_RETRIES, App.PIPELINE_DEFAULT_RETRY_AFTER_SECONDS);
            SpotifyAuthorization spotifyAuthorization = new SpotifyAuthorization(client, pipelineConfig);
            SparkSession sparkSession = SparkSession.builder()
                    .appName("SpotifyDEProject")
                    .master("local[*]")
                    .getOrCreate();
            SpotifyApiClient spotifyApiClient = new SpotifyApiClient(client, spotifyAuthorization, sparkSession, pipelineConfig);
            SpotifyCatalogClient spotifyClient = new SpotifyCatalogClient(spotifyApiClient, sparkSession);
            SpotifyAnalytics spotifyAnalytics = new SpotifyAnalytics();
            String playlistId = "6kgjElb9E4Kf6tJUFWXQdB";
            Dataset<Row> processedPlaylist = spotifyClient.processPlaylist(playlistId);

            Dataset<Row> albumTracks = spotifyClient.getDistinctTracksFromPlaylistAlbums(playlistId, processedPlaylist);
            List<String> tracksList = spotifyClient.getTracksList(playlistId, albumTracks);
            Dataset<Row> fullAlbumsFromPlaylist = spotifyClient.getFullTrackDetailsByPlaylist(playlistId, albumTracks, tracksList);
            Dataset<Row> tracksFeatures = spotifyAnalytics.deriveTrackFeatures(fullAlbumsFromPlaylist);

            fullAlbumsFromPlaylist.createOrReplaceTempView("full_albums");
            tracksFeatures.createOrReplaceTempView("full_albums_with_tracks_features");
        }
    }
}
