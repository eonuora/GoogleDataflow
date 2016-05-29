package com.googlecloud;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.windowing.*;
import com.googlecloud.transforms.WriteToBigQuery;
import com.googlecloud.utils.BQSchemaHelper;
import com.googlecloud.utils.BigQueryTableOptions;
import com.googlecloud.utils.DataFlowUtils;
import com.googlecloud.utils.PubsubTopicAndSubscriptionOptions;
import com.googlecloud.windowduration.ProcessWindowType;
import org.joda.time.Duration;

import java.io.IOException;

/**
 * Created by Ekene on 03-Apr-2016.
 */
public class StreamingPubSubToBigQueryPipeline {

    public static final Duration ONE_DAY = Duration.standardDays(1);
    public static final Duration ONE_HOUR = Duration.standardHours(1);
    public static final Duration TEN_SECONDS = Duration.standardSeconds(10);
    public static final int MAX_EVENTS_IN_FILE = 500000;

    private interface StreamingPubSubToBigQueryPipelineOptions extends PubsubTopicAndSubscriptionOptions, BigQueryTableOptions {
    }

//    static class StringToRowConverter extends DoFn<String, TableRow> {
//        /**
//         * In this example, put the whole string into single BigQuery field.
//         */
//        @Override
//        public void processElement(ProcessContext c) {
//
//            JSONObject jsonObj = null;
//            try {
//                jsonObj = new JSONObject(c.element());
//            } catch (JSONException e) {
//                e.printStackTrace();
//            }
//
//            TableRow row = new TableRow();
//
//            row.set("FinanceKey", jsonObj.get("FinanceKey"));
//            row.set("Date", jsonObj.get("DateKey"));
//            row.set("OrganizationName", jsonObj.get("OrganizationName"));
//            row.set("DepartmentGroupName", jsonObj.get("DepartmentGroupName"));
//            row.set("ScenarioName", jsonObj.get("ScenarioName"));
//            row.set("AccountDescription", jsonObj.get("AccountDescription"));
//            row.set("Amount", jsonObj.get("Amount"));
//
//
//            try {
//                java.util.Date date = new java.util.Date();
//                SimpleDateFormat sdf = new SimpleDateFormat();
//                sdf.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"));
//
//                java.util.Date UtcDate = sdf.parse(sdf.format(date));
//
//                long unixTime = UtcDate.getTime() / 1000;
//
//                row.set("ProcessTimestamp", unixTime);
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//            c.output(row);
//
//
//        }
//
//        static TableSchema getSchema() {
//            return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
//                // Compose the list of TableFieldSchema from tableSchema.
//                {
//                    add(new TableFieldSchema().setName("FinanceKey").setType("INTEGER"));
//                    add(new TableFieldSchema().setName("Date").setType("STRING"));
//                    add(new TableFieldSchema().setName("OrganizationName").setType("STRING"));
//                    add(new TableFieldSchema().setName("DepartmentGroupName").setType("STRING"));
//                    add(new TableFieldSchema().setName("ScenarioName").setType("STRING"));
//                    add(new TableFieldSchema().setName("AccountDescription").setType("STRING"));
//                    add(new TableFieldSchema().setName("Amount").setType("FLOAT"));
//
//                    add(new TableFieldSchema().setName("ProcessTimestamp").setType("TIMESTAMP"));
//
//                }
//            });
//        }
//    }

    public static void main(String[] args) {


        StreamingPubSubToBigQueryPipelineOptions options = PipelineOptionsFactory.create()
                //.withValidation()
                .as(StreamingPubSubToBigQueryPipelineOptions.class);
        options.setProject("gcloud-testing");
        options.setPubsubTopic("projects/gcloud-testing/topics/test_topic");
        options.setPubsubSubscription("projects/gcloud-testing/subscriptions/test_subscription");
        options.setStagingLocation("gs://gcloud_testing_staging");
        options.setTempLocation("gs://gcloud_testing_temp");
        options.setRunner(BlockingDataflowPipelineRunner.class);
        options.setZone("europe-west1-b");
        options.setStreaming(true);
        options.setBigQueryTable("FRecords");
        // options.setBigQuerySchema(StringToRowConverter.getSchema());
        options.setNumWorkers(1);
        options.setMaxNumWorkers(1);
        DataFlowUtils dataflowUtils = new DataFlowUtils(options);

        ProcessWindowType processWindowType = ProcessWindowType.FIVE_MINUTE;
        ProcessWindowType shardingWindowType = ProcessWindowType.FIVE_MINUTE;

        try {
            dataflowUtils.setup();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String tableSpec = new StringBuilder()
                .append(options.getProject()).append(":")
                .append(options.getBigQueryDataset()).append(".")
                .append(options.getBigQueryTable())
                .toString();


        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(PubsubIO.Read.named("Reading from PubSub").timestampLabel("timestamp_ms").subscription(options.getPubsubSubscription()))
//                .apply(Window.named("Window").<String>into(FixedWindows.of(Duration.standardMinutes(5)))
//                        .triggering(
//                                AfterWatermark.pastEndOfWindow()
//                        )
//                        .withAllowedLateness(Duration.standardHours(1))
//                        .a)

                .apply(Window.named("Window").<String>into(FixedWindows.of(Duration.standardMinutes(5)))
                        .triggering(
                                AfterWatermark.pastEndOfWindow()
                                        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.standardSeconds(30)))
                                        .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.standardSeconds(45))))
                        .withAllowedLateness(Duration.standardSeconds(60))
                        .discardingFiredPanes())
//                .apply(Window.named("Window").<KV<String, String>>into(FixedWindows.of(Duration.standardMinutes(5)))
//                        .triggering(
//                                AfterWatermark.pastEndOfWindow()
//                                        .withEarlyFirings(AfterPane.elementCountAtLeast(300))
//                                        .withLateFirings(AfterFirst.of(AfterPane.elementCountAtLeast(300),
//                                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(45)))))
//                        .withAllowedLateness(Duration.standardSeconds(60))
//                        .discardingFiredPanes())

//                .apply(ParDo.of(new StringToRowConverter()))

                .apply(new WriteToBigQuery(options.getBigQueryDataset()
                        , options.getBigQueryTable()
                        , BQSchemaHelper.getSchemaJSON(options.getBigQueryTable())
                        , shardingWindowType));

        pipeline.run();


    }

}
