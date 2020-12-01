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
import logging
import socket
import sys

import lucidoitdoit.exception
import lucidoitdoit.session
import lucidoitdoit.udf

"""LuciDoItServer is used to execute arbitrary python code on arbitrary data

Of course executing arbitrary python code is a dangerous, dangerous thing to do.  On the other
hand, do it.  Do it!  Do it do it do it.  It's your machine, who's gonna tell you want you can
and can't do with it?
"""


class LuciDoItServer(object):
    """Runs a server to execute python code"""

    def __init__(self, host: str, port: int, info_file: str = None) -> None:
        """
        Create a server.

        :param host: The host address to bind the server.
        :param port: The port to use, or 0 to pick any free port.
        :param info_file:  A file to save server information in.
        """
        self.host = host
        self.port = port
        self.__info_file = info_file
        self.udf_registry = lucidoitdoit.udf.LuciUdfRegistry()

    def __on_bind(self, socket):
        """Called when the server socket is bound."""
        self.port = socket.getsockname()[1]
        if self.__info_file:
            with open(self.__info_file, "w") as f:
                logging.info("Writing info file: %s", self.__info_file)
                f.write(str(self.port))
        logging.info("Listening: %s", socket.getsockname())

    def run(self) -> None:
        """Runs the UDF execution service for a single client

        This can only serve one client at a time.  No other clients can connect while that client
        is being served.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            self.__on_bind(s)
            shutdown_requested = False
            while not shutdown_requested:
                s.listen(0)
                try:
                    # addr is a tuple of host, port
                    connection, addr = s.accept()
                    logging.info("Connected: %s", addr)
                    with connection:
                        lucidoitdoit.session.LuciSimpleSession(
                            self.udf_registry, connection
                        ).serve()
                except lucidoitdoit.exception.LuciClientDisconnect:
                    logging.info("Client disconnected: %s", addr)
                except lucidoitdoit.exception.LuciShutdownRequested:
                    shutdown_requested = True
                    logging.info("Client requested shutdown: %s", addr)
                finally:
                    s.shutdown(0)

    def run_multi(self) -> None:
        """Runs the UDF execution service for a multiple simultaneous clients

        While one client is connected to the server, other clients can connect at the same time.
        """

        import socketserver

        class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
            def handle(inner_self):
                logging.info("Connected: %s", inner_self.client_address)
                # TODO error handling
                try:
                    lucidoitdoit.session.LuciSimpleSession(
                        self.udf_registry, inner_self.request
                    ).serve()
                except lucidoitdoit.exception.LuciClientDisconnect:
                    logging.info("Client disconnected: %s", inner_self.client_address)
                except lucidoitdoit.exception.LuciShutdownRequested:
                    logging.info(
                        "Client requested shutdown: %s", inner_self.client_address
                    )
                    inner_self.server.shutdown()

        class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
            pass

        server = ThreadedTCPServer((self.host, self.port), ThreadedTCPRequestHandler)
        self.__on_bind(server.socket)
        if sys.version_info.minor <= 5:
            # Python 3.5 doesn't have this as a context manager.
            server.serve_forever()
        else:
            with server:
                server.serve_forever()


if __name__ == "__main__":
    pass
