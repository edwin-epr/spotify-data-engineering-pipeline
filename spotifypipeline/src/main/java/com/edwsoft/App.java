package com.edwsoft;

import com.edwsoft.client.SpotifyAuthenticator;

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
            SpotifyAuthenticator spa = new SpotifyAuthenticator(client);
            String gat = spa.getAccessToken().orElse("No funcionó");
            System.out.println(gat);
        }
    }
}
