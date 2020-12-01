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
import socket
from typing import List

import lucidoitdoit.frame

"""LuciDoItClient is used to execute arbitrary python code on arbitrary data

Of course executing arbitrary python code is a dangerous, dangerous thing to do.  On the other
hand, do it.  Do it!  Do it do it do it.  It's your machine, who's gonna tell you want you can
and can't do with it?
"""


class LuciDoItClient(lucidoitdoit.frame.LuciFrame):
    """Client API for communicating with the server."""

    def __init__(
        self, host: str, port: int, connection: socket.SocketType = None
    ) -> None:
        self.connection = connection
        self.host = host
        self.port = port

    def __enter__(self):
        if self.connection is None:
            self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connection.connect((self.host, self.port))
        super().__init__(self.connection)
        # Code to start a new transaction
        return self

    def __exit__(self, type, value, traceback):
        self.connection.close()

    def send_code(self, code: str) -> str:
        self.connection.sendall(bytes([0]))
        self.write_framed_binary(code.encode("utf-8"))
        return self.read_framed_binary().decode("utf-8")

    def send_input(self, code_id: str, msg: str) -> List[str]:
        self.connection.sendall(bytes([1]))
        self.write_framed_binary(code_id.encode("utf-8"))
        self.write_framed_binary(msg.encode("utf-8"))
        while True:
            (code, payload) = self.read_coded_binary()
            if code == -1:
                break
            yield payload.decode("utf-8")

    def send_shutdown(self) -> str:
        self.connection.sendall(bytes([2]))
        return self.read_framed_binary().decode("utf-8")


if __name__ == "__main__":
    pass
