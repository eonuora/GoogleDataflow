package com.googlecloud.utils;

import com.google.cloud.dataflow.sdk.options.*;

/**
 * Created by Ekene on 03-Apr-2016.
 */
public interface PubsubTopicAndSubscriptionOptions extends PubsubTopicOptions {

    @Description("Pub/Sub subscription")
    @Default.InstanceFactory(PubsubSubscriptionFactory.class)
    String getPubsubSubscription();
    void setPubsubSubscription(String subscription);

    /**
     * Returns a default Pub/Sub subscription based on the project and the job names.
     */
    static class PubsubSubscriptionFactory implements DefaultValueFactory<String> {
        @Override
        public String create(PipelineOptions options) {

            DataflowPipelineOptions dataflowPipelineOptions = options.as(DataflowPipelineOptions.class);

            return "projects/" + dataflowPipelineOptions.getProject() + "/subscriptions/" + dataflowPipelineOptions.getJobName();
        }
    }

}
