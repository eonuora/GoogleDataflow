package com.googlecloud;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import org.apache.avro.reflect.Nullable;

import java.io.File;

/**
 * Created by Ekene on 27-Mar-2016.
 */
public class GroupingAndJoiningPipeline {

    @DefaultCoder(AvroCoder.class)
    static class FinancialRecord implements Comparable<FinancialRecord> {

        @Nullable Long FinanceKey;
        @Nullable String DateKey;
        @Nullable String OrganizationName;
        @Nullable String DepartmentGroupName;
        @Nullable String ScenarioName;
        @Nullable String AccountDescription;
        @Nullable Double Amount;

        public FinancialRecord() {
        }

        public FinancialRecord(Long financeKey, String dateKey, String organizationName, String departmentGroupName, String scenarioName, String accountDescription, Double amount)
        {
            this.FinanceKey = financeKey;
            this.DateKey = dateKey;
            this.OrganizationName = organizationName;
            this.DepartmentGroupName = departmentGroupName;
            this.ScenarioName = scenarioName;
            this.AccountDescription = accountDescription;
            this.Amount = amount;
        }

        @Override
        public int compareTo(FinancialRecord o) {

            if(this.DateKey.compareTo(o.DateKey) == 0) return 0;
            if(this.OrganizationName.compareTo(o.OrganizationName) == 0) return 0;
            if(this.DepartmentGroupName.compareTo(o.DepartmentGroupName) == 0) return 0;
            if(this.ScenarioName.compareTo(o.ScenarioName) == 0) return 0;
            if(this.AccountDescription.compareTo(o.AccountDescription) == 0) return 0;
            if(this.Amount.compareTo(o.Amount) == 0) return 0;
            if(this.FinanceKey.compareTo(o.FinanceKey) == 0) return 0;

            return 1;
        }
    }


    public static void main(String[] args) {


        String filePath = new File("").getAbsolutePath();
        System.out.println(filePath);

        Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());


        PCollection<KV<String, Iterable<FinancialRecord>>> part1 =
                p.apply(TextIO.Read.named("Read File").from(filePath + "\\TestFiles\\FactFinance_Part1.csv"))
                        .apply(ParDo.named("Get Word Count").of(new ReadRecords()))
                        .apply(GroupByKey.create());

        PCollection<KV<String, Iterable<FinancialRecord>>> part2 =
                p.apply(TextIO.Read.named("Read File").from(filePath + "\\TestFiles\\FactFinance_Part2.csv"))
                        .apply(ParDo.named("Get Word Count").of(new ReadRecords()))
                        .apply(GroupByKey.create());

        p.run();


    }

    static class ReadRecords extends DoFn<String, KV<String, FinancialRecord>> {

        @Override
        public void processElement(ProcessContext c) throws Exception {

            String[] line = c.element().split(",");

            FinancialRecord f = new FinancialRecord(Long.parseLong(line[0]), line[1], line[2], line[3], line[4], line[5], Double.parseDouble(line[6]));

            //System.out.println(line[5]);

            c.output(KV.of(line[5], f));

        }
    }

    static class GroupRecords extends DoFn<KV<String, Iterable<FinancialRecord>>, KV<String, Iterable<FinancialRecord>>>{
        @Override
        public void processElement(ProcessContext processContext) throws Exception {

        }
    }

}
