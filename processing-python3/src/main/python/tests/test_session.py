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

from lucidoitdoit.session import LuciAvroSession


class LuciAvroSessionTestSuite(unittest.TestCase):
    """Test cases for LuciAvroSession."""

    def test_schema_check(self):
        """Basic check on schema."""
        protocol_schema = LuciAvroSession.get_schema()
        self.assertIsNotNone(protocol_schema)
        self.assertEqual(protocol_schema.type, "record")
        self.assertEqual(len(protocol_schema.fields), 1)
        self.assertEqual(protocol_schema.fields[0].name, "all")
        self.assertEqual(protocol_schema.fields[0].type.type, "union")


if __name__ == "__main__":
    # logging.basicConfig(level=logging.DEBUG)
    unittest.main()
