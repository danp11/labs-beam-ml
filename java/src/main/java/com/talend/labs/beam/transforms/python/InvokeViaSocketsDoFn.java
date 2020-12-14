package com.talend.labs.beam.transforms.python;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InvokeViaSocketsDoFn extends DoFn<String, String> {
  private static final Logger LOG = LoggerFactory.getLogger(InvokeViaSocketsDoFn.class);

  /**
   * An internally generated UID unique to this DoFn. This is used as a session ID that
   * determines when a Python server is launched.
   *
   * For example, using a global static UID would share a server among all workers in a job
   * instead of one per DoFn.
   */
  private final String uid;
  private final String host;
  private Integer port;
  private final String code;
  private final String requirements;
  private final String serverInvokerPath;

  private Client client;
  private String codeId;

  InvokeViaSocketsDoFn(
      String host,
      @Nullable Integer port,
      String code,
      String requirements,
      String serverInvokerPath) {
    this.host = host;
    this.port = port;
    this.code = code;
    this.requirements = requirements;
    this.uid = PythonServerInvoker.createUid();
    this.serverInvokerPath = serverInvokerPath;
  }

  @Setup
  public void setup() throws IOException {
    // We start the target server that will process the requests
    PythonServerInvoker pythonServerInvoker = PythonServerInvoker.create(uid, serverInvokerPath);
    this.port = pythonServerInvoker.getPort();
    LOG.debug("Connecting to " + this.host + ":" + this.port);
    if (this.client == null) {
      this.client = new Client(host, port);
      this.codeId = client.registerCode(code);
    }
  }

  @StartBundle
  public void startBundle() {}

  @ProcessElement
  public void processElement(@Element String record, OutputReceiver<String> outputReceiver) {
    for (String output : client.execute(codeId, record)) {
      outputReceiver.output(output);
    }
  }

  @FinishBundle
  public void finishBundle() {}

  @Teardown
  public void teardown() {
    if (client != null)
      client.close();
  }
}
