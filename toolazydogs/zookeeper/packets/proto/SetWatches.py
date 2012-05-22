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

class SetWatches:
    def __init__(self, relativeZxid, dataWatches, existWatches, childWatches):
        self.relativeZxid = relativeZxid
        self.dataWatches = dataWatches
        self.existWatches = existWatches
        self.childWatches = childWatches

    def serialize(self, output_archive, tag):
        output_archive.start_record(tag)
        output_archive.write_long(self.relativeZxid, 'relativeZxid')
        output_archive.start_vector(self.dataWatches, 'dataWatches')
        if self.dataWatches != None:
            for e1 in self.dataWatches:
                output_archive.write_string(e1, 'e1')
        output_archive.end_vector(self.dataWatches, 'dataWatches')
        output_archive.start_vector(self.existWatches, 'existWatches')
        if self.existWatches != None:
            for e1 in self.existWatches:
                output_archive.write_string(e1, 'e1')
        output_archive.end_vector(self.existWatches, 'existWatches')
        output_archive.start_vector(self.childWatches, 'childWatches')
        if self.childWatches != None:
            for e1 in self.childWatches:
                output_archive.write_string(e1, 'e1')
        output_archive.end_vector(self.childWatches, 'childWatches')
        output_archive.end_record(tag)

    def deserialize(self, input_archive, tag):
        input_archive.start_record(tag)
        self.relativeZxid = input_archive.read_long('relativeZxid')
        len1 = input_archive.start_vector('dataWatches')
        if len1 != None:
            self.dataWatches = []
            for vidx1 in range(len1):
                e1 =  input_archive.read_string('e1')
                self.dataWatches.append(e1)
        else:
            self.dataWatches = None
        input_archive.end_vector('dataWatches')
        len1 = input_archive.start_vector('existWatches')
        if len1 != None:
            self.existWatches = []
            for vidx1 in range(len1):
                e1 =  input_archive.read_string('e1')
                self.existWatches.append(e1)
        else:
            self.existWatches = None
        input_archive.end_vector('existWatches')
        len1 = input_archive.start_vector('childWatches')
        if len1 != None:
            self.childWatches = []
            for vidx1 in range(len1):
                e1 =  input_archive.read_string('e1')
                self.childWatches.append(e1)
        else:
            self.childWatches = None
        input_archive.end_vector('childWatches')
        input_archive.end_record(tag)

    def __repr__(self):
        return 'SetWatches(%r, %r, %r, %r)' % (self.relativeZxid, self.dataWatches, self.existWatches, self.childWatches)

    def __eq__(self, other):
        return self.relativeZxid == other.relativeZxid and self.dataWatches == other.dataWatches and self.existWatches == other.existWatches and self.childWatches == other.childWatches

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.relativeZxid, self.dataWatches, self.existWatches, self.childWatches))
