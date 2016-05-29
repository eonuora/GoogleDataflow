package com.googlecloud.transforms;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.*;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.googlecloud.utils.ProcessWindowHelper;
import com.googlecloud.windowduration.ProcessWindowType;
import org.joda.time.DateTimeZone;

import org.joda.time.format.DateTimeFormat;
import org.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.SimpleTimeZone;


/**
 * Generate, format, and perform sharded write to BigQuery
 */

public class WriteToBigQuery extends PTransform<PCollection<String>, PDone> implements DoFn.RequiresWindowAccess {

    private static final Logger LOG = LoggerFactory.getLogger(WriteToBigQuery.class);
    private static final String UTC_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
    private static final String EVENT_TIMESTAMP_COLUMN = "EventTimestamp";
    private static final String PROCESS_TIMESTAMP_COLUMN = "ProcessTimestamp";
    private static final String PROCESS_WINDOW_COLUMN = "ProcessWindow";


    private String datasetName;
    private String tableName;
    private String tableSchema;
    private ProcessWindowType shardingWindowType;


    public WriteToBigQuery(String datasetName, String tableName, String tableSchema, ProcessWindowType shardingWindowType) {
        this.datasetName = datasetName;
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.shardingWindowType = shardingWindowType;
    }


    /**
     * Convert each event message into a BigQuery TableRow as specified by table schema.
     */
    protected class ConvertTOBQRowFn extends DoFn<String, TableRow> implements DoFn.RequiresWindowAccess {

        @Override
        public void processElement(ProcessContext c) {


            //Checks to see if a supplied event attribute is defined in the schema
            java.util.function.Function<String, Boolean> isKnownColumn = (columnName) -> {

                JSONArray jsonArr = new JSONObject(tableSchema).getJSONObject("schema").getJSONArray("fields");

                for (int n = 0; n < jsonArr.length(); n++)
                    if (jsonArr.getJSONObject(n).getString("name").equals(columnName))
                        return true;

                return false;
            };


            try {

                //Convert event message into a JSONObject
                JSONObject jsonObj = new JSONObject(c.element());

                Iterator<String> iterator = jsonObj.keys();

                TableRow row = new TableRow();

                //Loop though each attribute key in the event json
                while (iterator.hasNext()) {

                    String key = iterator.next();
                    String value = jsonObj.get(key).toString();

                    // Add event attribute to table row if its is defined in the schema
                    if (isKnownColumn.apply(key))
                        row.set(key, (value.toLowerCase().equals("null") ? "" : value));

                }

                // Add a event time to row if defined in the schema
                if (isKnownColumn.apply(EVENT_TIMESTAMP_COLUMN))
                    row.set(EVENT_TIMESTAMP_COLUMN, c.timestamp().toString());

                // Add a process window to row if defined in the schema
                if (isKnownColumn.apply(PROCESS_WINDOW_COLUMN))
                    row.set(PROCESS_WINDOW_COLUMN, c.window().toString());

                // Add a process time to row if defined in the schema
                if (isKnownColumn.apply(PROCESS_TIMESTAMP_COLUMN)) {
                    java.util.Date date = new java.util.Date();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS zzz");
                    sdf.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"));

                    row.set(PROCESS_TIMESTAMP_COLUMN, sdf.format(date));
                }


                // Set row as output
                c.output(row);

            } catch (Exception e) {
                LOG.error(e.getMessage());

                throw e;
            }

        }
    }

    /**
     * Reads a schema json and build and returns a BigQuery table schema representation
     *
     * @param schemaJson : String json schema
     * @return TableSchema
     */
    public static TableSchema getSchema(String schemaJson) {

        JSONObject jsonObj = new JSONObject(schemaJson);

        ArrayList<TableFieldSchema> schema = new ArrayList<>();


        if (schemaJson != null) {


            JSONArray rows = jsonObj.getJSONObject("schema").getJSONArray("fields");

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

    /**
     * PDone is the output of a PTransform that performs write to BiqQuery
     */
    @Override
    public PDone apply(PCollection<String> eventMessage) {

        PCollection<TableRow> rows = eventMessage.apply(ParDo.named("Convert events to BQ row").of(new ConvertTOBQRowFn()));

        //rows.apply(Window.into(FixedWindows.of(Duration.standardMinutes(10))));
        //.apply(Window.into(CalendarWindows.days(1)))

        return ProcessWindowHelper.applyWindowingStrategy(rows, shardingWindowType)

                .apply(BigQueryIO.Write.named("Write to BigQuery")
                        .withSchema(getSchema(tableSchema))
                        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                        .to(new SerializableFunction<BoundedWindow, String>() {
                            public String apply(BoundedWindow window) {
                                String dayString = DateTimeFormat.forPattern(ProcessWindowHelper.getWindowTimeFormat(shardingWindowType))
                                        .withZone(DateTimeZone.UTC)
                                        .print(((IntervalWindow) window).start());
                                return "gcloud-testing:" + datasetName + "." + tableName + "_" + dayString;
                            }
                        }));

    }
}
