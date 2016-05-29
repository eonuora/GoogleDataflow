package com.googlecloud.transforms;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.WriteChannel;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.joda.time.format.ISODateTimeFormat;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SimpleTimeZone;

/**
 * Generate, format, and write BigQuery table row information. Subclasses {@link WriteToBigQuery}
 * to require windowing; so this subclass may be used for writes that require access to the
 * context's window information.
 */
//public class WriteWindowPaneToGCS extends PTransform<PCollection<String>, PDone> {
//
//    private static final Logger LOG = LoggerFactory.getLogger(WriteWindowPaneToGCS.class);
//
//    private String datasetName;
//    private String tableName;
//    private String bucketName;
//    private String fileNamePrefix;
//
//    private final int MAX_SIZE_PER_FILE = 1000;
//
//   private List<String> bufferedMessages = new ArrayList<>();
//
//    public WriteWindowPaneToGCS(String datasetName, String tableName, String bucketName, String fileNamePrefix) {
//
//        this.datasetName = datasetName;
//        this.tableName = tableName;
//
//        this.bucketName = bucketName;
//        this.fileNamePrefix = fileNamePrefix;
//    }
//
//    public transient Storage storage;
//
//    {
//        init();
//    }
//
//    public void init() {
//        storage = StorageOptions.defaultInstance().service();
//    }
//
//    private void readObject(java.io.ObjectInputStream in)
//            throws IOException, ClassNotFoundException {
//        init();
//    }



public class WriteWindowPaneToGCS extends PTransform<PCollection<String>, PDone> implements DoFn.RequiresWindowAccess{

    private static final Logger LOG = LoggerFactory.getLogger(WriteWindowPaneToGCS.class);

    private String datasetName;
    private String tableName;
//    private String bucketName;
//    private String fileNamePrefix;



    public WriteWindowPaneToGCS(String datasetName, String tableName, String bucketName, String fileNamePrefix) {

        this.datasetName = datasetName;
        this.tableName = tableName;

//        this.bucketName = bucketName;
//        this.fileNamePrefix = fileNamePrefix;
    }


    /**
     * Convert each message into a BigQuery TableRow as specified by table schema.
     */
    protected class BuildRowFn extends DoFn<String, TableRow> implements DoFn.RequiresWindowAccess{

        @Override
        public void processElement(ProcessContext c) {

            //LOG.info("Payload:" + c.element().toString());

            try {

                JSONObject jsonObj = new JSONObject(c.element());

                TableRow row = new TableRow()
                        .set("FinanceKey", jsonObj.get("FinanceKey"))
                        .set("Date", jsonObj.get("DateKey"))
                        .set("OrganizationName", jsonObj.get("OrganizationName"))
                        .set("DepartmentGroupName", jsonObj.get("DepartmentGroupName"))
                        .set("ScenarioName", jsonObj.get("ScenarioName"))
                        .set("AccountDescription", jsonObj.get("AccountDescription"))
                        .set("Amount", jsonObj.get("Amount"))

                        .set("window", c.window().toString())
                        .set("isFirst", c.pane().isFirst())
                        .set("isLast", c.pane().isLast())
                        .set("timing", c.pane().getTiming().toString())
                        .set("event_time", c.timestamp().toString());


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

    /**
     * Build the output table schema.
     */
    protected TableSchema getSchema() {
        return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
            // Compose the list of TableFieldSchema from tableSchema.
            {
                add(new TableFieldSchema().setName("FinanceKey").setType("INTEGER"));
                add(new TableFieldSchema().setName("Date").setType("STRING"));
                add(new TableFieldSchema().setName("OrganizationName").setType("STRING"));
                add(new TableFieldSchema().setName("DepartmentGroupName").setType("STRING"));
                add(new TableFieldSchema().setName("ScenarioName").setType("STRING"));
                add(new TableFieldSchema().setName("AccountDescription").setType("STRING"));
                add(new TableFieldSchema().setName("Amount").setType("FLOAT"));

                add(new TableFieldSchema().setName("window").setType("STRING"));
                add(new TableFieldSchema().setName("isFirst").setType("BOOLEAN"));
                add(new TableFieldSchema().setName("isLast").setType("BOOLEAN"));
                add(new TableFieldSchema().setName("timing").setType("STRING"));
                add(new TableFieldSchema().setName("event_time").setType("STRING"));


                add(new TableFieldSchema().setName("ProcessTimestamp").setType("TIMESTAMP"));


            }
        });
    }

    @Override
    public PDone apply(PCollection<String> eventMessage) {
        return eventMessage
                .apply(ParDo.named("Convert event to BQ row").of(new BuildRowFn()))
                .apply(BigQueryIO.Write
                        .to(getTable(eventMessage.getPipeline(), datasetName, tableName))
                        .withSchema(getSchema())
                        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND));
    }

    /**
     * Utility to construct an output table reference.
     */
    static TableReference getTable(Pipeline pipeline, String datasetName, String tableName) {
        PipelineOptions options = pipeline.getOptions();
        TableReference table = new TableReference();
        table.setDatasetId(datasetName);
        table.setTableId(tableName);
        table.setProjectId(options.as(GcpOptions.class).getProject());
        return table;
    }
}

