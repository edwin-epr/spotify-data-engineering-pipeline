package com.edwsoft.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import tools.jackson.databind.JsonNode;

import java.util.Collections;
import java.util.Optional;

public class SpotifyDataProcessor {
    public static Dataset<Row> jsonToDataFrame(JsonNode jsonNode, SparkSession sparkSession) {
        String jsonString = Optional.ofNullable(jsonNode)
                .map(JsonNode::toString)
                .orElseThrow(()-> new RuntimeException("Json node is empty"));

        Dataset<String> jsonDataset = sparkSession.createDataset(
                Collections.singletonList(jsonString),
                Encoders.STRING()
        );

        return sparkSession.read().json(jsonDataset);
    }
}
