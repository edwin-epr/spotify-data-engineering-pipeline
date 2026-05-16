# Spotify Data Engineering Pipeline

A data engineering pipeline built in Java that extracts, transforms, and loads playlist data from the Spotify Web API using Apache Spark. This project is a work in progress — the storage layer (AWS) is currently under development.

---

## Motivation

This project was born out of a desire to build a real-world data engineering pipeline outside of an academic context, using an industry-relevant stack. Rather than following a tutorial verbatim, the original Python/Azure Databricks reference was reimplemented from scratch in Java with a local Spark setup, deliberate architectural decisions, and adaptation to Spotify's updated API constraints.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Data Processing | Apache Spark 4.0.1 (Spark SQL, Datasets) |
| Build Tool | Maven |
| HTTP Client | Java 11 HttpClient |
| JSON Parsing | Jackson (JsonMapper, ArrayNode) |
| Logging | SLF4J + Logback |
| Testing | JUnit 5, Mockito |
| Source API | Spotify Web API |
| Storage (WIP) | AWS (S3, to be defined) |

---

## Project Structure

```
src/main/java/com/edwsoft/
├── App.java                          # Entry point (used for testing during development)
├── catalog/
│   └── SpotifyCatalogClient.java     # Domain logic: playlist, track, album extraction
├── client/
│   ├── SpotifyApiClient.java         # HTTP client wrapper with batch support
│   └── SpotifyAuthorization.java     # OAuth2 Client Credentials flow
├── config/
│   └── PipelineConfig.java           # Environment variable loader and validator
├── exceptions/
│   └── HttpException.java            # Custom HTTP error handling
└── processor/
    └── SpotifyDataProcessor.java     # JSON to Spark Dataset conversion
```

---

## Architecture Overview

The pipeline is organized around three clear responsibilities:

**Extraction** is handled by `SpotifyApiClient`, which wraps Java's `HttpClient` to communicate with the Spotify API. It supports both single-resource fetching (`fetchJson`) and batch processing (`fetchBatchJson`), which iterates individually over a list of IDs since Spotify's batch endpoints were deprecated in 2024–2026.

**Transformation** lives in `SpotifyCatalogClient`, which applies domain knowledge to raw API responses — extracting track metadata, flattening artist arrays, and preparing DataFrames for downstream use.

**Conversion** is handled by `SpotifyDataProcessor`, which takes a generic `JsonNode` array and converts it into a typed Spark `Dataset<Row>`, keeping the transformation logic decoupled from the domain.

---

## Key Design Decisions

### `arrays_zip` over double `posexplode` + join

When extracting artist IDs and names from playlist tracks, an earlier approach used two separate `posexplode` operations followed by a join on row index. This was replaced with:

```java
processedPlaylist.selectExpr(
    "inline(arrays_zip(artists.id, artists.name)) as (artist_id, artist_name)"
)
```

`arrays_zip` combines both arrays element-by-element in memory before exploding, eliminating the shuffle cost of the join and halving the number of intermediate rows. This matters at scale.

### Fault-tolerant batch fetching

`fetchBatchJson` catches `HttpException` per individual request and returns an empty `JsonNode` instead of failing the entire stream. Empty nodes are filtered before building the Dataset. If all requests fail, an empty DataFrame is returned. This follows the **dead letter** pattern common in production pipelines: process what you can, log what fails.

```java
.map(spotifyId -> {
    try {
        return fetchJson(endpoint, spotifyId);
    } catch (HttpException e) {
        logger.error("Failed to fetch {}/{}: {}", endpoint, spotifyId, e.getMessage(), e);
        return createEmptyJsonObject();
    }
})
.filter(jsonNode -> !jsonNode.isEmpty())
```

### Single `processPlaylist` call

`processPlaylist` makes an HTTP request to Spotify and processes the response through Spark. Methods that depend on it (`getPlaylistsMetadata`, `getArtistsNameIds`) receive the resulting `Dataset<Row>` as a parameter rather than calling `processPlaylist` themselves. This avoids redundant HTTP calls and repeated Spark processing for the same playlist.

### Playlist ID as correlation parameter

Methods that operate on a processed Dataset still accept `playlistId` as a parameter — not for logic, but for log traceability. This makes it possible to correlate log entries across the pipeline to a specific playlist, which is useful in production debugging.

---

## Spotify API Constraints (as of 2026)

Several endpoints used in reference implementations are no longer available:

| Endpoint | Status |
|---|---|
| `GET /audio-features` | Removed (Nov 2024) |
| `GET /recommendations` | Removed (Nov 2024) |
| `GET /artists/top-tracks` | Removed |
| `GET /albums` (batch) | Removed |
| `GET /playlists/{id}/tracks` | Replaced by `/playlists/{id}/items` |

The pipeline was adapted to work exclusively with currently available endpoints. Batch operations are handled by iterating over individual resource endpoints.

---

## Logging

Logging is handled by SLF4J with Logback as the implementation. Log4j2 (bundled with Spark) is explicitly excluded from the build to avoid provider conflicts.

Logs are written simultaneously to the console and to a rotating daily file under `logs/`. Spark and Hadoop internal logs are filtered to `WARN` level to reduce noise.

```
logs/
└── spotify-pipeline.log     # Current log file (gitignored)
```

Log levels used:
- `INFO` — pipeline flow and successful operations
- `WARN` — Spark/Hadoop internal warnings
- `ERROR` — HTTP failures and exceptions, always with full stack trace
- `DEBUG` — internal processing details (e.g. row count before Dataset creation)

---

## Configuration

All configuration is loaded from environment variables. The pipeline fails fast with a descriptive error if any variable is missing or empty.

| Variable | Description |
|---|---|
| `SPOTIFY_CLIENT_ID` | Spotify app client ID |
| `SPOTIFY_CLIENT_SECRET` | Spotify app client secret |
| `SPOTIFY_REDIRECT_URI` | OAuth2 redirect URI |
| `SPOTIFY_BASE_URL` | Spotify API base URL (`https://api.spotify.com/v1`) |
| `SPOTIFY_TOKEN_URL` | Token endpoint (`https://accounts.spotify.com/api/token`) |

---

## Status

This project is actively under development. The extraction and transformation layers are functional. The storage layer (AWS S3 / Parquet or similar) is the next milestone.

Sections to be added once the pipeline is complete:
- Installation and setup instructions
- How to run
- Output examples and schema reference
- AWS architecture diagram
