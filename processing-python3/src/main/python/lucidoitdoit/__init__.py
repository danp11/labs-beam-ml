#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run a python server to execute user-defined functions.

Usage:
  lucidoitdoit [--verbose] server [--host=<ip:port>] [--multi] [--port-file=<portfile>]
  lucidoitdoit [--verbose] client [--host=<ip:port>] [--port-file=<portfile>] <CODE>
      <INPUT>... [--shutdown]
  lucidoitdoit (-h | --help)
  lucidoitdoit --version

Options:
  -h --help         Show this screen.
  --version         Show version.
  --host=<ip:port>  Host for the server [default: 127.0.0.1:60986].
  --verbose         Log more information while running.
  --shutdown        Request a shutdown after the client has finished running.
  --port-file=<pf>  If present on a server, save the port in the specified file.  Otherwise, load
                    the port from the file.
  --multi           Run the server in multi-threaded mode (EXPERIMENTAL).
  <CODE>            A (potentially multi-line) block of code to run over the inputs.
  <INPUT>           A list of input records as strings.

Examples:

lucidoitdoit server --host :50008

  Runs a server on the 50008 port of the local machine (on all IP addresses)

lucidoitdoit client --host :50008 'output = input.upper().split(" ")' "a B" c

  Executes the code on the server and returns three lines: A, B and C

"""
import logging
import sys
import traceback

from docopt import docopt

import lucidoitdoit.client
import lucidoitdoit.server


def main(opts: dict = None) -> None:
    if opts is None:
        opts = docopt(__doc__, version="0.1")

    # Common options
    if opts["--verbose"]:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    logging.debug("docopts: %s", str(opts))
    host_and_port = opts["--host"].split(":", 2)
    host = host_and_port[0]
    port = int(host_and_port[1] if len(host_and_port) > 1 else 0)
    portfile = opts["--port-file"]

    # Specific commands
    if opts["server"]:
        if opts["--multi"]:
            lucidoitdoit.server.LuciDoItServer(host, port, portfile).run_multi()
        else:
            lucidoitdoit.server.LuciDoItServer(host, port, portfile).run()

    elif opts["client"]:
        if portfile:
            with open(portfile, "r") as p:
                port = int(p.readline())

        if port == 0:
            raise Exception("No port specified.")

        code = opts.get("<CODE>")
        inputs = opts.get("<INPUT>")
        with lucidoitdoit.client.LuciDoItClient("", port) as client:
            logging.info("Connected: %s", client.connection.getsockname())

            code_id = client.send_code(code)
            logging.info("Registered: %s", code_id)

            for input in inputs:
                for output in client.send_input(code_id, input):
                    logging.info(output)

            if opts["--shutdown"]:
                logging.info("Shutting down: %s", client.send_shutdown())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(__doc__)
        print(e)
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
