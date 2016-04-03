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
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.apache.avro.reflect.Nullable;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by Ekene on 27-Mar-2016.
 */
public class GroupingAndJoiningPipeline {

    final static TupleTag<Iterable<FinancialRecord>> p1 = new TupleTag<>();
    final static TupleTag<Iterable<FinancialRecord>> p2 = new TupleTag<>();

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
                        .apply(ParDo.named("Get Part 1").of(new ReadRecords()))
                        .apply(GroupByKey.create());

        PCollection<KV<String, Iterable<FinancialRecord>>> part2 =
                p.apply(TextIO.Read.named("Read File").from(filePath + "\\TestFiles\\FactFinance_Part2.csv"))
                        .apply(ParDo.named("Get Part 2").of(new ReadRecords()))
                        .apply(GroupByKey.create());


        PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
                KeyedPCollectionTuple.of(p1, part1)
                        .and(p2, part2)
                        .apply(CoGroupByKey.create());

        coGbkResultCollection.apply(new WriteRecords(filePath + "\\TestFiles\\Output\\Part1andPart2.csv"));

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

    static class WriteRecords extends PTransform< PCollection<KV<String, CoGbkResult>>, PDone> {

        private String output;

        public  WriteRecords(String output)
        {
            this.output = output;
        }

        @Override
        public PDone apply(PCollection<KV<String, CoGbkResult>> input) {

            PCollection<String> pc = input.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
                @Override
                public void processElement(ProcessContext c) throws Exception {

                    String o = c.element().getKey() ;

                    for( Iterable<FinancialRecord> n : c.element().getValue().getAll(p1))
                        for ( FinancialRecord f : n )
                            o += "\n\t" + f.Amount;

                    for( Iterable<FinancialRecord> n : c.element().getValue().getAll(p2))
                        for ( FinancialRecord f : n )
                            o += "\n\t" + f.Amount;

                    c.output(o);

                }
            }));

            return pc.apply(TextIO.Write.to(output));
        }
    }

}
