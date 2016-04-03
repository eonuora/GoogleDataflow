package com.googlecloud.utils;

import com.google.cloud.dataflow.sdk.options.*;

/**
 * Created by Ekene on 03-Apr-2016.
 */
public interface PubsubTopicOptions extends DataflowPipelineOptions {

    @Description("Pub/Sub topic")
    @Default.InstanceFactory(PubsubTopicFactory.class)
    String getPubsubTopic();

    void setPubsubTopic(String topic);

    /**
     * Returns a default Pub/Sub topic based on the project and the job names.
     */
    static class PubsubTopicFactory implements DefaultValueFactory<String> {
        @Override
        public String create(PipelineOptions options) {
            DataflowPipelineOptions dataflowPipelineOptions = options.as(DataflowPipelineOptions.class);
            return "projects/" + dataflowPipelineOptions.getProject() + "/topics/" + dataflowPipelineOptions.getJobName();
        }
    }

}
