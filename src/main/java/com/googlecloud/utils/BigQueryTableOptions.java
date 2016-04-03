package com.googlecloud.utils;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.options.*;

/**
 * Created by Ekene on 03-Apr-2016.
 */
public interface BigQueryTableOptions extends DataflowPipelineOptions{

    @Description("BigQuery dataset name")
    @Default.String("gcloud_testing_examples")
    String getBigQueryDataset();
    void setBigQueryDataset(String dataset);

    @Description("BigQuery table name")
    @Default.InstanceFactory(BigQueryTableFactory.class)
    String getBigQueryTable();
    void setBigQueryTable(String table);

    @Description("BigQuery table schema")
    TableSchema getBigQuerySchema();
    void setBigQuerySchema(TableSchema schema);

    /**
     * Returns the job name as the default BigQuery table name.
     */
    static class BigQueryTableFactory implements DefaultValueFactory<String> {
        @Override
        public String create(PipelineOptions options) {
            return options.as(DataflowPipelineOptions.class).getJobName()
                    .replace('-', '_');
        }
    }
}
