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


class LuciBaseException(Exception):
    """Base class of all exceptions raised by this code."""

    pass


class LuciUdfSandboxException(LuciBaseException):
    """Indicate that a suspicious UDF was provided for evaluation."""

    pass


class LuciClientDisconnect(LuciBaseException):
    """Indicate that the other side of the socket disconnected from the connection."""

    pass


class LuciShutdownRequested(LuciBaseException):
    """Indicate that the client requested that the server be shut down cleanly."""

    pass


if __name__ == "__main__":
    pass
