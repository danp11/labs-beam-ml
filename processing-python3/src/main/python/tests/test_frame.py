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
import unittest
from socket import SocketType

import avro

from lucidoitdoit.frame import LuciFrame


class MockEmptySocket(SocketType):
    """A socket that reads infinite zeros."""

    def __init__(self, buffer_size: int) -> None:
        self.buffer = bytes(buffer_size)

    def recv(self, buffer_size: int) -> bytes:
        return self.buffer if buffer_size >= len(self.buffer) else bytes(buffer_size)


class MockSocket(SocketType):
    """A mock socket that reads and writes to bytes."""

    def __init__(self, buffer: bytes = None) -> None:
        self.buffer = bytes() if buffer is None else bytes(buffer)

    def recv(self, buffer_size: int) -> bytes:
        result = self.buffer[0:buffer_size]
        self.buffer = self.buffer[buffer_size:]
        return result

    def sendall(self, data: bytes):
        self.buffer = self.buffer + data


class LuciFrameTestSuite(unittest.TestCase):
    """Test cases for LuciFrame."""

    def test_read_binary(self):
        frame = LuciFrame(MockEmptySocket(buffer_size=100))
        result = frame.read_binary(10)
        self.assertEqual(10, len(result))
        result = frame.read_binary(1000)
        self.assertEqual(1000, len(result))

    def test_read_framed_binary(self):
        mock_socket = MockSocket(buffer=b"\x00\x00\x00\x03abc")
        result = LuciFrame(mock_socket).read_framed_binary()
        self.assertEqual(b"abc", result)

    def test_read_empty_framed_binary(self):
        mock_socket = MockSocket(buffer=b"\x00\x00\x00\x00")
        result = LuciFrame(mock_socket).read_framed_binary()
        self.assertEqual(b"", result)

    def test_read_coded_binary(self):
        mock_socket = MockSocket(buffer=b"\x00\x00\x00\x03abc")
        result = LuciFrame(mock_socket).read_coded_binary()
        self.assertEqual((3, b"abc"), result)

    def test_read_datum(self):
        mock_socket = MockSocket(buffer=b"\x00\x00\x00\x04\x06ABC")
        result = LuciFrame(mock_socket).read_datum(avro.schema.parse('"string"'))
        self.assertEqual("ABC", result)

    def test_write_response_code(self):
        mock_socket = MockSocket()
        LuciFrame(mock_socket).write_response_code(1234)
        self.assertEqual(b"\x00\x00\x04\xd2", mock_socket.buffer)

    def test_write_framed_binary(self):
        mock_socket = MockSocket()
        LuciFrame(mock_socket).write_framed_binary(b"abc")
        self.assertEqual(b"\x00\x00\x00\x03abc", mock_socket.buffer)

    def test_write_empty_framed_binary(self):
        mock_socket = MockSocket()
        LuciFrame(mock_socket).write_framed_binary(b"")
        self.assertEqual(b"\x00\x00\x00\x00", mock_socket.buffer)

    def test_write_write_datum(self):
        mock_socket = MockSocket()
        LuciFrame(mock_socket).write_datum("ABC", avro.schema.parse('"string"'))
        self.assertEqual(b"\x00\x00\x00\x04\x06ABC", mock_socket.buffer)

    if __name__ == "__main__":
        # logging.basicConfig(level=logging.DEBUG)
        unittest.main()
