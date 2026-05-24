package com.edwsoft;

import com.edwsoft.catalog.SpotifyAnalytics;
import com.edwsoft.catalog.SpotifyCatalogClient;
import com.edwsoft.client.SpotifyApiClient;
import com.edwsoft.client.SpotifyAuthorization;
import com.edwsoft.config.PipelineConfig;
import com.edwsoft.processor.SpotifyDataProcessor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Arrays;
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
            String playlistId = "1qtUv9fHzbQG9KMMzKfMgl";
            Dataset<Row> processedPlaylist = spotifyClient.processPlaylist(playlistId);
            Dataset<Row> albumTracks = spotifyClient.getAlbumTracksFromPlaylists(playlistId, processedPlaylist);
            List<String> tacksList = spotifyClient.getTracksList(playlistId, albumTracks);
            Dataset<Row> tracksBasedOnPlaylist = spotifyClient.getAlbumsTracksBasedOnPlaylist(playlistId, albumTracks, tacksList);
            tracksBasedOnPlaylist.show();
        }
    }
}
