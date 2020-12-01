package com.talend.labs.beam.transforms.python;

import java.io.IOException;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Ignore
@RunWith(JUnit4.class)
public class PythonTransformIT {

  public interface PythonTransformITOptions extends PipelineOptions {
    @Description("Server invoker path")
    String getServerInvokerPath();

    void setServerInvokerPath(String path);
  }

  private static PythonTransformITOptions options;
  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() throws IOException {
    PipelineOptionsFactory.register(PythonTransformITOptions.class);
    options = TestPipeline.testingPipelineOptions().as(PythonTransformITOptions.class);
    String integrationTestPipelineOptions = System.getenv("integrationTestPipelineOptions");
    System.out.println(integrationTestPipelineOptions);
    System.out.println(options.getServerInvokerPath());
    System.exit(0);
  }

  @Test
  public void testUppercase() {
    PCollection<String> input =
        testPipeline.apply(Create.of("Africa,Algeria,,Algiers,1,1,1995,64.2"));

    PCollection<String> uppercase =
        input.apply(
            "uppercase",
            PythonTransform.of(
                "output = [input for i in range(0,5)]", "", options.getServerInvokerPath()));

    PCollection<String> fiveTimes =
        input.apply(
            "fiveTimes", PythonTransform.of("output=input", "", options.getServerInvokerPath()));

    PAssert.thatSingleton(fiveTimes.apply("Count All", Count.<String>globally()))
        .isEqualTo((long) 1);

    testPipeline.run().waitUntilFinish();
  }
}
