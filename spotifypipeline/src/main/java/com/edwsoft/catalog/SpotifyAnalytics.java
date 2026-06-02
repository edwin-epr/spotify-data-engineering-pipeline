package com.edwsoft.catalog;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpotifyAnalytics {
    private static final Logger logger = LoggerFactory.getLogger(SpotifyAnalytics.class);

    public Dataset<Row> albumsWithMostTracks(Dataset<Row> albumTracks) {
        logger.info("Calculating albums with most tracks");

        return albumTracks
                .groupBy("album_id", "album_name")
                .count()
                .orderBy(functions.desc("count"));
    }

    public Dataset<Row> avgPopularityByAlbum(Dataset<Row> albumTracks) {
        logger.info("Calculating average popularity by album");

        return albumTracks
                .groupBy("album_id", "album_name")
                .agg(functions.avg("popularity").alias("avg_popularity"))
                .orderBy(functions.desc("avg_popularity"));
    }

    public Dataset<Row> top5LongestAlbums(Dataset<Row> albumTracks) {
        logger.info("Calculating top 5 longest albums by track count");

        return albumTracks
                .groupBy("album_id", "album_name")
                .agg(functions.count("track_id").alias("track_count"))
                .orderBy(functions.desc("track_count"))
                .limit(5);
    }

    public Dataset<Row> getTrackFeatures(Dataset<Row> albumTracks) {
        logger.info("Extracting track features from album tracks.");

        return albumTracks.select(
                functions.col("track_id"),
                functions.col("track_name"),
                functions.col("artist_name"),
                functions.col("album_name"),
                functions.col("release_date"),
                functions.col("duration_seconds"),
                functions.col("popularity"),
                functions.col("track_number"),
                functions.year(functions.to_date(
                        functions.col("release_date"), "yyyy-MM-dd"))
                        .alias("release_year")
        );
    }
}
