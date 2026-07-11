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
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        try(HttpClient client = HttpClient.newHttpClient()) {
            PipelineConfig pipelineConfig = new PipelineConfig();
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
            Dataset<Row> fullAlbumsFromPlaylist = spotifyClient.getAlbumsTracksBasedOnPlaylist(playlistId, albumTracks, tracksList);
            Dataset<Row> tracksFeatures = spotifyAnalytics.getTrackFeatures(fullAlbumsFromPlaylist);

            fullAlbumsFromPlaylist.createOrReplaceTempView("full_albums");
            tracksFeatures.createOrReplaceTempView("full_albums_with_tracks_features");
        }
    }
}
