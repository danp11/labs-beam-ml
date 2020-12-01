package com.talend.labs.beam.transforms.python;

import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PythonTransform extends PTransform<PCollection<String>, PCollection<String>> {
  private static final Logger LOG = LoggerFactory.getLogger(PythonTransform.class);

  private final String host;
  private final Integer port;
  private final String code;
  private final String requirements;
  private final String serverInvokerPath;

  private PythonTransform(
      String host,
      @Nullable Integer port,
      String code,
      String requirements,
      String serverInvokerPath) {
    this.host = host;
    this.port = port;
    this.code = code;
    this.requirements = requirements;
    this.serverInvokerPath = serverInvokerPath;
  }

  /**
   * @param code Python script to be executed
   * @param requirements contents of requirements.txt file to setup a virtualenv
   * @param serverInvokerPath path to Python server invoker
   * @return
   */
  public static PythonTransform of(String code, String requirements, String serverInvokerPath) {
    return new PythonTransform("localhost", null, code, requirements, serverInvokerPath);
  }

  @Override
  public PCollection<String> expand(PCollection<String> input) {
    // Uncomment to test with SDK Harness
    //    ByteString pythonTransformPayload =
    //        ExpansionResolver.getPythonPTransformCode(
    //            input, "talend:labs:ml:genreclassifier:python:v1", new byte[0], "localhost:9097");
    //    return input.apply(ParDo.of(new InvokeViaSdkHarnessDoFn(pythonTransformPayload)));
    if (serverInvokerPath != null) {
      LOG.info("Execute Python code via Python server");
      return input.apply(
          ParDo.of(new InvokeViaSocketsDoFn(host, port, code, requirements, serverInvokerPath)));
    } else {
      LOG.info("Execute Python code via Jython");
      return input.apply(ParDo.of(new InvokeViaJythonDoFn(code)));
    }
  }
}
