package com.edwsoft;

import com.edwsoft.client.SpotifyAuthenticator;
import com.edwsoft.config.PipelineConfig;

import java.net.http.HttpClient;

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
            SpotifyAuthenticator spa = new SpotifyAuthenticator(client, pipelineConfig);
            String gat = spa.getAccessToken().orElse("No funcionó");
            System.out.println(gat);
        }
    }
}
