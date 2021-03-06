
package com.googlecloud;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.*;

import java.io.File;


public class SimpleFileReadWritePipeline {

    final static TupleTag<String> allWordsTag = new TupleTag<>();
    final static TupleTag<String> wordsAboveAverageTag = new TupleTag<>();
    final static TupleTag<String> wordsBelowAverageTag = new TupleTag<>();

    public static void main(String[] args) {

        String filePath = new File("").getAbsolutePath();

        Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

        //String[] a = {"Hello", "World!"};
        //p.apply(Create.of(a))

        PCollectionTuple all = p.apply(TextIO.Read.named("Read File").from(filePath + "\\TestFiles\\words.txt"))
                .apply(new CountWordsCT());


        all.get(allWordsTag).apply(TextIO.Write.named("Write All Words").to(filePath + "\\TestFiles\\Output\\allwords.txt"));
        all.get(wordsAboveAverageTag).apply(TextIO.Write.named("Write wordsAboveAverage").to(filePath + "\\TestFiles\\Output\\wordsAboveAverage.txt"));
        all.get(wordsBelowAverageTag).apply(TextIO.Write.named("Write wordsBelowAverage").to(filePath + "\\TestFiles\\Output\\wordsBelowAverage.txt"));

       // p.apply(TextIO.Read.named("Read File").from("C:\\TestFiles\\m.txt"))

                /*//* Java 8 lambda implementation of split word, count, concatenation and capitalisation *//*
                //.apply(FlatMapElements.via((String w) -> Arrays.asList( w.split("[^a-zA-Z']+"))).withOutputType(new TypeDescriptor<String>() {}))
                //.apply(MapElements.via((String w) -> w + ": " + String.valueOf(w.length())).withOutputType(new TypeDescriptor<String>() {}))
                //.apply(MapElements.via((String w) -> w.toUpperCase()).withOutputType(new TypeDescriptor<String>() { }))

                /* Java normal implementation of split word, count, concatenation and capitalisation
                .apply(ParDo.named("ReadingLines").of(new ExtractWordsFn()))
                .apply(ParDo.of(new WordCountFn()))
                .apply(ParDo.of(new WordToUpperCaseFn()))*/

                /*Composite Transformation to implementation split word, count, concatenation and capitalisation  */


              //  .apply(TextIO.Write.to("C:\\TestFiles\\Output\\b.txt"));


        p.run();

    }

    static class WordToUpperCaseFn extends DoFn<String, String> {

        @Override
        public void processElement(ProcessContext c) throws Exception {
            c.output(c.element().toUpperCase());
            //LOG.info(c.element());
        }
    }

    static class WordCountFn extends DoFn<String, String> {
        @Override
        public void processElement(ProcessContext p) throws Exception {

            String s = p.element() + ": " + String.valueOf(p.element().length());
            p.output(s);
            System.out.println(s);
        }
    }

    static int count = 1;

    static class ExtractWordsFn extends DoFn<String, String> {
        @Override
        public void processElement(ProcessContext c) throws Exception {

            String b = String.valueOf(count);
            count += 1;
            System.out.println(b + ": " + c.element().toString());

            for (String word : c.element().toString().split("[^a-zA-Z']+")) {
                if (!word.isEmpty()) {
                    c.output(word);
                }
            }
        }
    }

    public static class CountWordsCT extends PTransform<PCollection<String>, PCollectionTuple> {

        @Override
        public PCollectionTuple apply(PCollection<String> input) {


            PCollection<String> words = input.apply(ParDo.named("Extract Word").of(new ExtractWordsFn()));

            PCollection<Integer> wordCount = words.apply(ParDo.named("Extract Count").of(new DoFn<String, Integer>() {
                @Override
                public void processElement(ProcessContext c) throws Exception {
                    c.output(c.element().length());
                }
            }));

            final PCollectionView<Double> averageLength =
                    wordCount.apply(Mean.globally())
                            .apply(View.asSingleton());

            PCollection<String> wordsAboveAverage = words.apply(ParDo.named("Get Words Above Average").withSideInputs(averageLength).of(new DoFn<String, String>() {
                @Override
                public void processElement(ProcessContext c) throws Exception {

                    if (c.element().length() >= c.sideInput(averageLength))
                        c.output(c.element() + "" + c.element().length());

                }
            }));

            PCollection<String> wordsBelowAverage = words.apply(ParDo.named("Get Words Below Average").withSideInputs(averageLength).of(new DoFn<String, String>() {
                @Override
                public void processElement(ProcessContext c) throws Exception {

                    if (c.element().length() < c.sideInput(averageLength))
                        c.output(c.element() + "" + c.element().length());
                }
            }));


            /*PCollectionTuple output = words.apply(ParDo.withSideInputs(averageLength).withOutputTags(allWordsTag, TupleTagList.of(wordsAboveAverageTag).and(wordsBelowAverageTag)).of(new DoFn<String,String>() {
                @Override
                public void processElement(ProcessContext c)  {

                    String word = c.element();
                    c.output(word);

                    if (word.length() >= c.sideInput(averageLength))
                        c.sideOutput(wordsAboveAverageTag, c.element());
                    else
                        c.sideOutput(wordsBelowAverageTag, c.element());
                }
            }));*/

            //return output;

            return PCollectionTuple.of(allWordsTag, words)
                    .and(wordsAboveAverageTag, wordsAboveAverage)
                    .and(wordsBelowAverageTag, wordsBelowAverage);
        }
    }


}

