# File generated by hadoop record compiler. Do not edit.
"""
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""

class ExistsRequest:
    def __init__(self, path, watch):
        self.type = 3
        self.path = path
        self.watch = watch

    def serialize(self, output_archive, tag):
        output_archive.start_record(tag)
        output_archive.write_string(self.path, 'path')
        output_archive.write_bool(self.watch, 'watch')
        output_archive.end_record(tag)

    def deserialize(self, input_archive, tag):
        input_archive.start_record(tag)
        self.path = input_archive.read_string('path')
        self.watch = input_archive.read_bool('watch')
        input_archive.end_record(tag)

    def __repr__(self):
        return 'ExistsRequest(%r, %r)' % (self.path, self.watch)

    def __eq__(self, other):
        return self.path == other.path and self.watch == other.watch

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.path, self.watch))
