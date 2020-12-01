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

import ast
import logging
import uuid
from typing import Any


class LuciUdfRegistry(object):
    @staticmethod
    def udfize_def_string(code: str) -> str:
        """Return a string with the code as a function"""
        return """\
def udf(input):
 output = None
 {}
 return output
""".format(
            " ".join(line for line in code.splitlines(True))
        )

    @staticmethod
    def udfize_def(input: str, glbCtx: dict = None, lclCtx: dict = None):
        if lclCtx is None:
            lclCtx = {}
        if glbCtx is None:
            glbCtx = {}

        udf_ast = ast.parse(LuciUdfRegistry.udfize_def_string(input), filename="<udf>")

        finder = UdfSecurityChecker()
        finder.visit(udf_ast)

        exec(compile(udf_ast, filename="<udf>", mode="exec"), glbCtx, lclCtx)
        return lclCtx["udf"]

    def __init__(self) -> None:
        """Initialize the internal dictionary."""
        self.log = logging.getLogger(__name__)
        self.udf_registry = {}

    def put(self, code: str) -> bytes:
        id = str(uuid.uuid4()).encode("utf-8")
        try:
            self.udf_registry[id] = LuciUdfRegistry.udfize_def(code)
        except SyntaxError as e:
            # This might not be an accurate assessment of the position of the error.
            line = e.lineno - 2
            column = e.offset - 1
            self.log.debug("Guessing line and column: %s:%s", line, column)
            raise e
        return id

    def exec(self, id: bytes, input: str) -> Any:
        return self.udf_registry[id](input)


class UdfSecurityChecker(ast.NodeVisitor):
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.usesDoubleUnderscore = False
        self.ximport = []
        self.ximportfrom = []

    def generic_visit(self, node: ast.AST) -> Any:
        self.log.debug("AST: %s", node)
        super().generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> Any:
        if node.attr.startswith("__"):
            self.usesDoubleUnderscore = True
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> Any:
        for alias in node.names:
            self.ximport.append(alias.name)
            self.log.debug("IMPORTING: %s", alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
        for alias in node.names:
            self.ximportfrom.append(alias.name)
            self.log.debug("IMPORTING FROM: %s", alias.name)
        self.generic_visit(node)

    def is_probably_valid(self) -> bool:
        return not self.usesDoubleUnderscore
