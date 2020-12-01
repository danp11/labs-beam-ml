import io
import struct
from socket import SocketType
from typing import Any, Tuple

import avro
import avro.io
from avro.schema import Schema

from lucidoitdoit import exception

"""Helper methods for reading and writing from a connection."""


class LuciFrame(object):
    """Helper methods for reading and writing from a connection."""

    def __init__(self, connection: SocketType) -> None:
        self.connection = connection

    def read_binary(self, length: int) -> bytes:
        """Blocking call to read the specified number of bytes from the connection."""
        block = b""
        while length > 0:
            # This is a blocking call and will continue to wait until the client disconnects.
            data = self.connection.recv(length)
            if len(data) == 0:
                raise exception.LuciClientDisconnect()
            length -= len(data)
            if block == b"" and length == 0:
                return data
            block += data
        return block

    def read_framed_binary(self):
        """Blocking call to read the next frame of bytes from the connection."""
        (length,) = struct.unpack("!i", self.read_binary(4))
        return self.read_binary(length)

    def read_coded_binary(self) -> Tuple[int, bytes]:
        """Blocking call to read the next frame of bytes from the connection."""
        (length,) = struct.unpack("!i", self.read_binary(4))
        if length < 0:
            return (length, bytes(0))
        return (length, self.read_binary(length))

    def read_datum(self, schema: Schema) -> Any:
        """Blocking call to read the next frame of Avro bytes from the connection."""
        reader = io.BytesIO(self.read_framed_binary())
        decoder = avro.io.BinaryDecoder(reader)
        datum_reader = avro.io.DatumReader(schema)
        return datum_reader.read(decoder)

    def write_response_code(self, code: int) -> None:
        """Write a single integer to the connection."""
        self.connection.sendall(struct.pack("!i", code))

    def write_framed_binary(self, data: bytes, length: int = None) -> None:
        """Write binary data to the connection."""
        if length is None:
            length = len(data)
        self.connection.sendall(struct.pack("!i", length))
        self.connection.sendall(data)

    def write_datum(self, datum: Any, schema: Schema) -> None:
        """Write a single Avro datum to the connection."""
        bio = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bio)
        datum_writer = avro.io.DatumWriter(schema)
        datum_writer.write(datum, encoder)
        self.write_framed_binary(bio.getvalue())
