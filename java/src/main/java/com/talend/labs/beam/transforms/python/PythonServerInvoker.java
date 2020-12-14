package com.talend.labs.beam.transforms.python;

import org.apache.beam.runners.fnexecution.environment.ProcessManager;
import org.apache.beam.runners.fnexecution.environment.ProcessManager.RunningProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;

/**
 * Class used to instantiate the python executable. It manages virtualenv setup and server
 * invocation.
 */
class PythonServerInvoker {
  private static final Logger LOG = LoggerFactory.getLogger(PythonServerInvoker.class);

  /** Used to construct a session ID. */
  private static final char[] RND = "abcdefghijklmnopqrstvwxyz0123456789".toCharArray();

  private static final int UID_LENGTH = 16;

  /**
   * Override the location where the python whl can be loaded from. TODO: find dynamically not hard
   * coded.
   */
  // private static final String WHEEL =
  // "/home/rskraba/working/github/labs-beam-ml/lucidoitdoit/dist/lucidoitdoit-0.1-py3-none-any.whl";
  private static final String WHEEL = null;

  private static final boolean WAIT_FOR_SOCKET = true;

  private static PythonServerInvoker instance = null;

  private final String uid;

  // We reuse Beam's ProcessManager because it has the logic to redirect I/O
  // TODO Decide if we create a specialized copy of this to not bring the full Beam
  // core-construction dependency
  private ProcessManager processManager;

  private Integer port;
  private final String processId = "PythonServerInvoker";

  private static boolean isAvailable(int port) {
    try (Socket ignored = new Socket("localhost", port)) {
      return false;
    } catch (IOException ignored) {
      return true;
    }
  }

  /**
   * We don't use new ServerSocket(0) because it binds the port to the Java process so it is
   * unreliable.
   *
   * @return -1 if not port found.
   */
  private static int findFreePort() {
    for (int i = 50000; i < 65535; i++) {
      if (!isAvailable(i)) {
        continue;
      }
      return i;
    }
    return -1;
  }

  private PythonServerInvoker(String uid, String serverInvokerPath) {
    this.uid = uid;
    processManager = ProcessManager.create();

    HashMap<String, String> env = new HashMap<>();
    if (WHEEL != null) {
      env.put("LUCIDOITDOIT_WHL", WHEEL);
    }

    try {
      if (WAIT_FOR_SOCKET) {
        RunningProcess runningProcess =
            processManager.startProcess(
                processId, serverInvokerPath, Arrays.asList("start", this.uid), env);

        // Wait until the socket file appears and then read it
        Path socket = Paths.get("/tmp", "luci", uid, ".socket");
        LOG.debug("Trying to read socket info at " + socket);
        int attempts = 60;
        while (attempts-- > 0) {
          if (Files.exists(socket)) {
            try (Scanner scanner = new Scanner(socket)) {
              port = scanner.nextInt();
              break;
            }
          } else {
            Thread.sleep(1000);
          }
        }

        LOG.info("Server available at " + this.port);
        if (this.port == null) {
          //TODO should we clean up the environment here?
          throw new RuntimeException("Could not find available python server socket");
        }
        // The process exits by itself, running python in the background
        // runningProcess.isAliveOrThrow();
      } else {
        this.port = findFreePort();
        RunningProcess runningProcess =
            processManager.startProcess(
                processId,
                serverInvokerPath,
                Arrays.asList("server", "--host=localhost:" + this.port, "--multi"),
                new HashMap<>());
        runningProcess.isAliveOrThrow();
      }

      LOG.info("Started driver program");
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static synchronized PythonServerInvoker create(String uid, String serverInvokerPath) {
    if (instance == null) {
      instance = new PythonServerInvoker(uid, serverInvokerPath);
    }
    return instance;
  }

  /** @return a session ID for the PythonServerInvoker. */
  public static String createUid() {
    StringBuilder uid = new StringBuilder("luci");
    Random rnd = new Random();
    for (int i = 0; i < UID_LENGTH; i++) uid.append(RND[rnd.nextInt(RND.length)]);
    return uid.toString();
  }

  public Integer getPort() {
    return port;
  }
}
