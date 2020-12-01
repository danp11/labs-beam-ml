package com.talend.labs.beam.transforms.python;

import org.apache.beam.sdk.transforms.DoFn;
import org.python.core.PyObject;
import org.python.core.PyUnicode;
import org.python.util.PythonInterpreter;

import java.io.IOException;

/**
 * Very primitive way to invoke Python code with Jython, it should be used only for
 * testing/benchmarking.
 */
public class InvokeViaJythonDoFn extends DoFn<String, String> {

  private PythonInterpreter interpreter = null;

  private PyObject pyFn = null;

  private String code = "";

  public InvokeViaJythonDoFn(String code) {
    this.code = code;
  }

  @Setup
  public void setup() throws Exception {
    interpreter = new PythonInterpreter();
    String userData = setUpMap();
    interpreter.exec(userData);
    pyFn = interpreter.get("userFunction");
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    PyObject output = pyFn.__call__(new PyUnicode(context.element()));
    context.output(output.toString());
  }

  private String setUpMap() {
    return ""
            + "def userFunction(input):\n" //
            + "  " + code.replaceAll("\n", "\n  ").trim() + "\n"//
            + "  return output\n"; //
  }

  @Teardown
  public void tearDown() {
    interpreter.close();
  }
}
