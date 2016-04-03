package com.googlecloud;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineWorkerPoolOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.*;
import com.googlecloud.utils.BigQueryTableOptions;
import com.googlecloud.utils.DataFlowUtils;
import com.googlecloud.utils.PubsubTopicAndSubscriptionOptions;
import com.googlecloud.utils.PubsubTopicOptions;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.ArrayList;


/**
 * Created by Ekene on 03-Apr-2016.
 */
public class StreamingPubSubToBigQueryPipeline {

    private interface StreamingPubSubToBigQueryPipelineOptions extends PubsubTopicAndSubscriptionOptions, BigQueryTableOptions {
    }

    static class StringToRowConverter extends DoFn<String, TableRow> {
        /**
         * In this example, put the whole string into single BigQuery field.
         */
        @Override
        public void processElement(ProcessContext c) {
            c.output(new TableRow().set("string_field", c.element().toString()));
        }

        static TableSchema getSchema() {
            return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
                // Compose the list of TableFieldSchema from tableSchema.
                {
                    add(new TableFieldSchema().setName("string_field").setType("STRING"));
                }
            });
        }
    }

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
        options.setBigQuerySchema(StringToRowConverter.getSchema());
        options.setNumWorkers(1);
        options.setMaxNumWorkers(1);
        DataFlowUtils dataflowUtils = new DataFlowUtils(options);

        try{
            dataflowUtils.setup();
        }catch(IOException e){
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
                .apply(Window.named("Window").<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                        .triggering(
                                AfterWatermark.pastEndOfWindow()
                                        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.standardSeconds(30)))
                                        .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.standardSeconds(45))))
                        .withAllowedLateness(Duration.standardSeconds(60))
                        .accumulatingFiredPanes())
                /*.apply(ParDo.of(new DoFn<String, String>() {

                    Integer count = 1;

                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                        c.output(count + "\t" + c.element());

                        count++;
                    }
                }))*/
                .apply(ParDo.of(new StringToRowConverter()))
                .apply(BigQueryIO.Write.named("Write to BigQuery").to(tableSpec)
                        .withSchema(StringToRowConverter.getSchema()));

        pipeline.run();



    }

}
