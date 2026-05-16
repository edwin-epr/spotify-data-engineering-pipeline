package com.edwsoft;

import com.edwsoft.catalog.SpotifyCatalogClient;
import com.edwsoft.client.SpotifyApiClient;
import com.edwsoft.client.SpotifyAuthorization;
import com.edwsoft.config.PipelineConfig;
import com.edwsoft.processor.SpotifyDataProcessor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
            // Probar token
//            String gat = spotifyAuthorization.getAccessToken().orElse("No funcionó");
//            System.out.println(gat);
            // Ejemplo 1: Obtener un track específico
//          JsonNode track = spotifyApiClient.fetchJson("tracks", "3n3Ppam7vgaVa1iaRUc9Lp");
//          System.out.println(track);
//          System.out.println("Track name: " + track.get("name").asString());
            // Ejemplo 2: Procesar múltiples tracks
//            List<String> trackIds = List.of(
//                    "3n3Ppam7vgaVa1iaRUc9Lp",  // Mr. Brightside - The Killers
//                    "37vVp2sWHuuIBOSl1NswP6",   // I'm yours - Isabel LaRosa
//                    "3cfOd4CMv2snFaKAnMdnvK"   // September - Earth, Wind & Fire
//            );
//            Dataset<Row> tracksDataSet = spotifyApiClient.fetchBatchJson("tracks", trackIds);
//            tracksDataSet.show();
            // Ejemplo 3: Obtener un track específico
//          JsonNode track = spotifyApiClient.fetchJson("tracks", "3n3Ppam7vgaVa1iaRUc9Lp");
//          System.out.println(track);
//          System.out.println("Track name: " + track.get("name").asString());
//          Dataset<Row> dataset = SpotifyDataProcessor.jsonToDataFrame(track, sparkSession);
//          dataset.show();
            Dataset<Row> processedPlaylist = spotifyClient.processPlaylist("1qtUv9fHzbQG9KMMzKfMgl");
//            Dataset<Row> artistsDataset = spotifyClient.getArtistsNameIds("1qtUv9fHzbQG9KMMzKfMgl", processedPlaylist);
//            artistsDataset.show();
//            Dataset<Row> playlistMetadataDataset = spotifyClient.getPlaylistsMetadata("1qtUv9fHzbQG9KMMzKfMgl", processedPlaylist);
//            playlistMetadataDataset.show();
//            List<String> albumsIds = Arrays.asList("4iDsJtesBbWiGaarx04mVO","4zSeBpHmi7WGKDYYkqZWjf");
//            Dataset<Row> albumsDataset = spotifyClient.getAlbums(albumsIds);
//            albumsDataset.printSchema();
//            albumsDataset.show();
            Dataset<Row> albumsFromPlaylist = spotifyClient.getAlbumsFromPlaylists("1qtUv9fHzbQG9KMMzKfMgl", processedPlaylist);
            albumsFromPlaylist.show();
        }
    }
}
