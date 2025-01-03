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
from typing import Optional

from pookeeper import WatcherEventType


class WatcherEvent:
    def __init__(self,
                 event_type: Optional[WatcherEventType] = None,
                 state: Optional[int] = None,
                 path: Optional[str] = None):
        self.event_type = event_type
        self.state = state
        self.path = path

    def serialize(self, output_archive, tag):
        output_archive.start_record(tag)
        output_archive.write_int(self.event_type, 'type')
        output_archive.write_int(self.state, 'state')
        output_archive.write_string(self.path, 'path')
        output_archive.end_record(tag)

    def deserialize(self, input_archive, tag):
        input_archive.start_record(tag)
        self.event_type = WatcherEventType(input_archive.read_int('type'))
        self.state = input_archive.read_int('state')
        self.path = input_archive.read_string('path')
        input_archive.end_record(tag)

    def __repr__(self):
        return 'WatcherEvent(%r, %r, %r)' % (self.event_type, self.state, self.path)

    def __eq__(self, other):
        return self.event_type == other.event_type and self.state == other.state and self.path == other.path

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.event_type, self.state, self.path))
