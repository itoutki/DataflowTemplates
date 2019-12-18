package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.templates.common.BigQueryConverters.BigQueryReadOptions;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;

public class BigQueryRunQuery {

    public interface BigQueryToBigQueryOptions
            extends BigQueryReadOptions {
        @Description("JSON file with BigQuery Schema description")
        ValueProvider<String> getJSONPath();

        void setJSONPath(ValueProvider<String> value);

        @Description("Output topic to write to")
        ValueProvider<String> getOutputTable();

        void setOutputTable(ValueProvider<String> value);

        @Validation.Required
        @Description("Temporary directory for BigQuery loading process")
        ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

        void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
    }

    private static final String BIGQUERY_SCHEMA = "BigQuery Schema";
    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String MODE = "mode";

    public static void main(String[] args) {
        BigQueryToBigQueryOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToBigQueryOptions.class);
        String query = "SELECT "
        + "ITEM.id AS itemId, ITEM.name AS itemName, ITEM.ownerId AS ownerId, USER.name AS ownerName "
        + "FROM "
        + "dataset001.item001 AS ITEM "
        + "LEFT OUTER JOIN "
        + "dataset001.user001 AS USER "
        + "ON "
        + "ITEM.ownerId = USER.id WHERE ITEM.ownerId = '0';";

        String query2 = "CREATE OR REPLACE TABLE dataset001.item001 (id STRING, name STRING, ownerId STRING);";
        String query3 = "CREATE TEMP FUNCTION customGreeting(a STRING) RETURNS STRING LANGUAGE js AS \"\"\" var d = new Date(); if (d.getHours() < 12) { return 'Good Morning, ' + a + '!'; } else { return 'Good Evening, ' + a + '!'; }\"\"\"; SELECT customGreeting(names) as everyone FROM UNNEST([\"Hannah\", \"Max\", \"Jakob\"]) AS names;";

        Pipeline pipeline = Pipeline.create(options);

        PCollection<TableRow> inputRows = pipeline.apply("ReadFromBigQuery", BigQueryIO.readTableRows()
                .fromQuery(query3).usingStandardSql());

        pipeline.run();
    }
}
