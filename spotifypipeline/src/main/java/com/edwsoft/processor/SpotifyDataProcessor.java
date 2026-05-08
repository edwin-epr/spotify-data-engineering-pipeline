package com.edwsoft.processor;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SpotifyDataProcessor {
    private static final Logger logger = LoggerFactory.getLogger(SpotifyDataProcessor.class);
    public static Dataset<Row> jsonToDataFrame(JsonNode jsonNode, SparkSession sparkSession) {
        Objects.requireNonNull(jsonNode, "JsonNode is null.");
        if (jsonNode.isNull()) {
            throw new RuntimeException("JsonNode contains a JSON null value.");
        }
        if (!jsonNode.isArray()) {
            throw new RuntimeException("JsonNode is not an array, got: " + jsonNode.getNodeType());
        }
        if (jsonNode.isEmpty()) {
            throw new RuntimeException("JsonNode array is empty.");
        }

        List<String> jsonRows = new ArrayList<>();
        jsonNode.forEach(item -> jsonRows.add(item.toString()));

        logger.debug("Converting {} JSON rows to Dataset", jsonRows.size());

         Dataset<String> jsonDataset = sparkSession.createDataset(jsonRows, Encoders.STRING());

        return sparkSession.read().json(jsonDataset);
    }
}
