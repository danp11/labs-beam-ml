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

import avro
import avro.schema
import pkg_resources

import lucidoitdoit.exception
import lucidoitdoit.frame
import lucidoitdoit.udf

"""On the server, tools for managing a session with a single connected client"""


class LuciSession(lucidoitdoit.frame.LuciFrame):
    """Base class for managing sessions with a connected client."""

    def __init__(
        self,
        udf_registry: lucidoitdoit.udf.LuciUdfRegistry,
        connection: socket.SocketType,
    ) -> None:
        """Create a session to interact with the client.

        :param udf_registry: The UDF registry that the client interacts with.
        :param connection: The socket that the client is connected with.
        """
        super().__init__(connection)
        self.log = logging.getLogger(__name__)
        self.udf_registry = udf_registry
        # The default UDF currently selected by the client.
        self.default_udf_id = None

    def serve(self) -> None:
        """Start running the session.

        A subclass implementation should block until the session is over."""
        raise SystemError("Error: missing protocol implementation.")


class LuciSimpleSession(LuciSession):
    """A client session that uses a simplified protocol based on strings."""

    def __run_00_register_code(self) -> None:
        """Registers code to be executed as a udf."""
        code = self.read_framed_binary().decode("utf-8")
        udf_id = self.udf_registry.put(code)
        logging.info("Registering code as %s", udf_id.decode("utf-8"))
        self.write_framed_binary(udf_id)
        self.default_udf_id = udf_id

    def __run_01_execute(self) -> None:
        """Executes a udf on a given record."""
        udf_id = self.read_framed_binary()
        if len(udf_id) == 0:
            udf_id = self.default_udf_id
        input = self.read_framed_binary().decode("utf-8")
        out = self.udf_registry.exec(udf_id, input)

        if isinstance(out, str):
            self.write_framed_binary(out.encode("utf-8"))
        elif isinstance(out, list):
            for outstr in out:
                self.write_framed_binary(outstr.encode("utf-8"))
        else:
            raise SystemError("Unknown type {}".format(type(out)))

        self.write_response_code(-1)

    def __run_02_request_server_shutdown(self) -> None:
        """Requests a server shutdown."""
        self.write_framed_binary("OK".encode("utf-8"))
        raise lucidoitdoit.exception.LuciShutdownRequested()

    def serve(self):
        """Communicate with the client on the given socket.

        This blocks until the server or client disconnects (and a LuciClientDisconnect exception
        is thrown.
        """
        while True:
            # The first byte determines what command is run
            cmd = self.read_binary(length=1)
            if cmd[0] == 0:
                self.__run_00_register_code()
            elif cmd[0] == 1:
                self.__run_01_execute()
            elif cmd[0] == 2:
                self.__run_02_request_server_shutdown()
            else:
                raise SystemError("Unknown command {}".format(cmd[0]))


class LuciAvroSession(LuciSession):
    """Helper methods for reading and writing from a connection."""

    protocol_schema = None

    @staticmethod
    def get_schema() -> avro.schema.RecordSchema:
        """The schema used to communicate between the client and server."""
        if LuciAvroSession.protocol_schema is None:
            avsc = pkg_resources.resource_string(__name__, "protocol.avsc")
            if sys.version_info.minor <= 5:
                avsc = str(avsc, "utf-8")

            LuciAvroSession.protocol_schema = avro.schema.parse(avsc)
        return LuciAvroSession.protocol_schema

    def serve(self):
        """Communicate with the client on the given socket.

        This blocks until the server or client disconnects (and a LuciClientDisconnect exception
        is thrown.
        """
        while True:
            # The first byte determines what command is run
            cmd = self.read_datum(LuciAvroSession.get_schema())
            raise SystemError("Unknown command {}".format(cmd[0]))


if __name__ == "__main__":
    pass
