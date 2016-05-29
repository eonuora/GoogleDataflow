package com.googlecloud;


import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.windowing.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.googlecloud.transforms.WriteWindowPaneToGCS;
import com.googlecloud.utils.BigQueryTableOptions;
import com.googlecloud.utils.DataFlowUtils;
import com.googlecloud.utils.PubsubTopicAndSubscriptionOptions;
import org.joda.time.Duration;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class PubSubGcsSSCCEPipepline {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubGcsSSCCEPipepline.class);

    public static final String BUCKET_PATH = "dataflow-requests";

    public static final String BUCKET_NAME = "gcloud_testing_temp";

    public static final Duration ONE_DAY = Duration.standardDays(1);
    public static final Duration ONE_HOUR = Duration.standardHours(1);
    public static final Duration TEN_SECONDS = Duration.standardSeconds(10);

    public static final int MAX_EVENTS_IN_FILE = 1000;

    public static final String PUBSUB_SUBSCRIPTION = "projects/gcloud-testing/subscriptions/test_subscription";

    private interface StreamingPubSubToBigQueryPipelineOptions extends PubsubTopicAndSubscriptionOptions, BigQueryTableOptions {
    }

    private static class DoGCSWrite extends DoFn<Iterable<String>, Void> implements DoFn.RequiresWindowAccess {


        public transient Storage storage;

        {
            init();
        }

        public void init() {
            storage = StorageOptions.defaultInstance().service();
        }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            init();
        }

        @Override
        public void processElement(ProcessContext c) throws Exception {
            String isoDate = ISODateTimeFormat.dateTime().print(c.window().maxTimestamp());
            long paneIndex = c.pane().getIndex();
            String blobName = String.format("%s/%s/%s", BUCKET_PATH, isoDate, paneIndex);

            BlobId blobId = BlobId.of(BUCKET_NAME, blobName);

            LOG.info("writing pane {} to blob {}", paneIndex, blobName);
            WriteChannel writer = storage.writer(BlobInfo.builder(blobId).contentType("text/plain").build());
            LOG.info("blob stream opened for pane {} to blob {} ", paneIndex, blobName);
            int i = 0;
            for (Iterator<String> it = c.element().iterator(); it.hasNext(); ) {
                i++;
                writer.write(ByteBuffer.wrap(it.next().getBytes()));
                LOG.info("wrote {} elements to blob {}", i, blobName);
            }
            writer.close();
            LOG.info("sucessfully write pane {} to blob {}", paneIndex, blobName);
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
        // options.setBigQuerySchema(StringToRowConverter.getSchema());
        options.setNumWorkers(1);
        options.setMaxNumWorkers(1);
        DataFlowUtils dataflowUtils = new DataFlowUtils(options);

        try {
            dataflowUtils.setup();
        } catch (IOException e) {
            e.printStackTrace();
        }


        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(PubsubIO.Read.named("Reading from PubSub").timestampLabel("timestamp_ms").subscription(options.getPubsubSubscription()))
                .apply(WithKeys.of(new SerializableFunction<String, String>() {
                    public String apply(String s) {
                        return "constant";
                    }
                }))


                .apply(Window.<KV<String, String>>into(FixedWindows.of(ONE_HOUR))
                        .withAllowedLateness(ONE_DAY)
                        .triggering(AfterWatermark.pastEndOfWindow()
                                .withEarlyFirings(AfterPane.elementCountAtLeast(MAX_EVENTS_IN_FILE))
                                .withLateFirings(AfterFirst.of(AfterPane.elementCountAtLeast(MAX_EVENTS_IN_FILE),
                                        AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(TEN_SECONDS))))
                        .discardingFiredPanes())

                .apply(GroupByKey.create())


                .apply(Values.<Iterable<String>>create())


                .apply(ParDo.named("Write to GCS").of(new DoGCSWrite()));

        pipeline.run();
    }

}