package client;

import com.edwsoft.client.SpotifyAuthorization;
import com.edwsoft.config.PipelineConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SpotifyAuthorizationTest {

    @Mock
    HttpClient httpClient;

    @Mock
    HttpResponse<String> httpResponse;

    @Mock
    PipelineConfig pipelineConfig;

    SpotifyAuthorization spotifyAuthorization;

    @BeforeEach
    public void setup() {
        spotifyAuthorization = new SpotifyAuthorization(httpClient, pipelineConfig);
        when(pipelineConfig.getClientId()).thenReturn("test-client-id");
        when(pipelineConfig.getClientSecret()).thenReturn("test-client-secret");
        when(pipelineConfig.getTokenUrl()).thenReturn("https://accounts.spotify.com/api/token");
    }

    @Test
    public void testGetTokenSuccess() throws IOException, InterruptedException {
        String mockResponse = "{\"access_token\":\"BQDtW6...\",\"token_type\":\"Bearer\",\"expires_in\":3600}";

        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(mockResponse);
        when(httpClient.send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>> any()))
                .thenReturn(httpResponse);

        Optional<String> token = spotifyAuthorization.getAccessToken();

        assertTrue(token.isPresent());
        assertFalse(token.get().isEmpty());
        verify(httpClient, times(1))
                .send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>> any());
    }

    @Test
    public void testGetTokenErrorStatus() throws IOException, InterruptedException {
        when(httpResponse.statusCode()).thenReturn(400);
        when(httpClient.send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>> any()))
                .thenReturn(httpResponse);

        Optional<String> token = spotifyAuthorization.getAccessToken();

        assertTrue(token.isEmpty());
    }

    @Test
    public void testGetTokenInterruptedException() throws IOException, InterruptedException {

        when(httpClient.send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>> any()))
                .thenThrow(new InterruptedException("Thread interrupted"));

        Optional<String> token = spotifyAuthorization.getAccessToken();

        assertTrue(token.isEmpty());
    }

    @Test
    public void testGetTokenI0Exception() throws IOException, InterruptedException {

        when(httpClient.send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>> any()))
                .thenThrow(new IOException("Network error"));

        Optional<String> token = spotifyAuthorization.getAccessToken();

        assertFalse(token.isPresent());
    }
}
