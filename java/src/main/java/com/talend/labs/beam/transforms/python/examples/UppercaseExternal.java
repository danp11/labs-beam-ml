package com.talend.labs.beam.transforms.python.examples;

import org.apache.beam.runners.core.construction.External;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class UppercaseExternal {
  private static final String URN = "talend:labs:ml:uppercase:python:v1";

  /** Specific pipeline options. */
  public interface UppercaseExternalPipelineOptions extends PipelineOptions {
    @Description("Input path")
    String getInputPath();

    void setInputPath(String path);

    @Description("Expansion Service URL")
    @Default.String("localhost:8097")
    String getExpansionServiceURL();

    void setExpansionServiceURL(String url);

  }

  public static void main(String[] args) {
    UppercaseExternalPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(UppercaseExternalPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    PCollection<String> input =
        (options.getInputPath() == null)
            ? p.apply(Create.of("Africa,Algeria,,Algiers,1,1,1995,64.2"))
            : p.apply(TextIO.read().from(options.getInputPath()));

    PCollection<KV<String, String>> uppercase =
        input.apply(
            External.of(URN, new byte[] {}, options.getExpansionServiceURL())
                .<KV<String, String>>withOutputType());

    uppercase.apply(ParDo.of(new PrintFn<>()));

    p.run().waitUntilFinish();
  }

  private static class PrintFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> out) {
      System.out.println("JAVA OUTPUT: " + element);
      out.output(element);
    }
  }
}
