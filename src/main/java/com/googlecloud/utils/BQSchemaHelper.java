package com.googlecloud.utils;


import com.google.api.services.bigquery.model.TableFieldSchema;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import com.google.api.services.bigquery.model.TableSchema;
import org.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for parsring and creating BiqQuery table schemas
 */
public class BQSchemaHelper {

    private static final Logger LOG = LoggerFactory.getLogger(BQSchemaHelper.class);
    private static final String schemaFilePathT = "schemas/%s_schema.json";

    /**
     * Reads a schema json file and returns a String representation
     *
     * @param schemaName : Should match the name of the event and have corresponding schema located in resource/schema
     * @return String : String representation of schema.
     */
    public static String getSchemaJSON(String schemaName) {

        String schemaPath = String.format(schemaFilePathT, schemaName);

        InputStream inputStream = BQSchemaHelper.class.getClassLoader().getResourceAsStream(schemaPath);

        try {
            if (inputStream != null) {

                BufferedReader streamReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
                StringBuilder responseStrBuilder = new StringBuilder();

                String inputStr;
                while ((inputStr = streamReader.readLine()) != null)
                    responseStrBuilder.append(inputStr);

                return responseStrBuilder.toString();

            } else {
                throw new FileNotFoundException("Schema file '" + schemaPath + "' not found in the classpath");
            }
        } catch (Exception e) {
            LOG.error("Exception: " + e);
        }

        return null;
    }

    /**
     * Reads a schema json file and build and returns a BigQuery table schema representation
     *
     * @param schemaName : Should match the name of the event and have corresponding schema located in resource/schema
     * @return TableSchema
     */
    public static TableSchema getSchema(String schemaName) {

        JSONObject jsonObject = new JSONObject(getSchemaJSON(schemaName));

        ArrayList<TableFieldSchema> schema = new ArrayList<>();


        if (jsonObject != null) {

            JSONArray rows = jsonObject.getJSONObject("schema").getJSONArray("fields");

            for (int n = 0; n < rows.length(); n++) {

                JSONObject row = rows.getJSONObject(n);
                schema.add(new TableFieldSchema()
                        .setName(row.getString("name"))
                        .setType(row.getString("type"))
                        .setMode(row.getString("mode")));
            }
        }

        return new TableSchema().setFields(schema);

    }
}