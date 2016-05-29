package com.googlecloud.transforms;

/**
 * Created by ekene on 5/29/16.
 */

/**
 * Created by ekene on 5/27/16.
 */

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.SimpleTimeZone;

/**
 * Generate, format, and write BigQuery table row information.
 */
public class RCCC<T> extends PTransform<PCollection<T>, PDone> {

    private static final Logger LOG = LoggerFactory.getLogger(WriteToBigQuery.class);

    private String datasetName;
    private String tableName;
    private TableSchema tableSchema;

    public RCCC(String datasetName, String tableName, TableSchema tableSchema) {

        this.datasetName = datasetName;
        this.tableName = tableName;
        this.tableSchema = tableSchema;
    }

    /**
     * Convert each message into a BigQuery TableRow as specified by table schema.
     */
    private class BuildRowFn extends DoFn<T, TableRow> {

        @Override
        public void processElement(ProcessContext c) {

            LOG.info(c.element().toString());

            try {

                JSONObject jsonObj = new JSONObject(c.element().toString());

                TableRow row = new TableRow();

                Iterator<String> iterator = jsonObj.keys();


                while (iterator.hasNext()) {

                    String key = iterator.next();
                    String value = jsonObj.get(key).toString();

                    if (!(key.equals("Category") || key.equals("MessageType") || key.equals("TimeStamp")))
                        row.set(key, (value.toLowerCase().equals("null") ? "" : value));

                }

                java.util.Date date = new java.util.Date();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS zzz");
                sdf.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"));

                row.set("ProcessTimestamp", sdf.format(date));

                c.output(row);


            } catch (Exception e) {
                LOG.error(e.getMessage());
            }

        }
    }


    @Override
    public PDone apply(PCollection<T> eventMessage) {
        return eventMessage
                .apply(ParDo.named("Convert event to BQ row").of(new BuildRowFn()))
                .apply(BigQueryIO.Write
                        .to(getTable(eventMessage.getPipeline(), datasetName, tableName))
                        .withSchema(tableSchema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    }

    /**
     * Utility to construct an output table reference.
     */
    private static TableReference getTable(Pipeline pipeline, String datasetName, String tableName) {
        PipelineOptions options = pipeline.getOptions();
        TableReference table = new TableReference();
        table.setDatasetId(datasetName);
        table.setTableId(tableName);
        table.setProjectId(options.as(GcpOptions.class).getProject());
        return table;
    }
}

