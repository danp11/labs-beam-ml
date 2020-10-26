package com.talend.labs.beam.transforms.python;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class PythonTransform extends PTransform<PCollection<String>, PCollection<String>> {

  private String code;
  private String requirements;

  private PythonTransform(String code, String requirements) {
    this.code = code;
    this.requirements = requirements;
  }

  // TODO might we do requirements a path better so it gets the requirements from some FS?

  /**
   * @param code Python script to be executed
   * @param requirements contents of requirements.txt file to setup a virtualenv
   * @return
   */
  public static PythonTransform of(String code, String requirements) {
    return new PythonTransform(code, requirements);
  }

  @Override
  public PCollection<String> expand(PCollection<String> input) {
    //    return input.apply(ParDo.of(new InvokeViaSdkHarnessDoFn()));
    return input.apply(ParDo.of(new InvokeViaSocketsDoFn(code, requirements)));
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    String code =
        "import nltk\n"
            + "\n"
            + "tokenized_text=sent_tokenize(input['sentence'])\n"
            + "print(tokenized_text)\n";
    String requirements = "nltk==3.5";
    PCollection<String> names =
        p.apply(Create.of("Maria", "John", "Xavier", "Erika"))
            .apply(PythonTransform.of(code, requirements))
            .apply(ParDo.of(new PrintFn<>()));
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
