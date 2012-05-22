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

class Stat:
    def __init__(self, czxid, mzxid, ctime, mtime, version, cversion, aversion, ephemeralOwner, dataLength, numChildren, pzxid):
        self.czxid = czxid
        self.mzxid = mzxid
        self.ctime = ctime
        self.mtime = mtime
        self.version = version
        self.cversion = cversion
        self.aversion = aversion
        self.ephemeralOwner = ephemeralOwner
        self.dataLength = dataLength
        self.numChildren = numChildren
        self.pzxid = pzxid

    def serialize(self, output_archive, tag):
        output_archive.start_record(tag)
        output_archive.write_long(self.czxid, 'czxid')
        output_archive.write_long(self.mzxid, 'mzxid')
        output_archive.write_long(self.ctime, 'ctime')
        output_archive.write_long(self.mtime, 'mtime')
        output_archive.write_int(self.version, 'version')
        output_archive.write_int(self.cversion, 'cversion')
        output_archive.write_int(self.aversion, 'aversion')
        output_archive.write_long(self.ephemeralOwner, 'ephemeralOwner')
        output_archive.write_int(self.dataLength, 'dataLength')
        output_archive.write_int(self.numChildren, 'numChildren')
        output_archive.write_long(self.pzxid, 'pzxid')
        output_archive.end_record(tag)

    def deserialize(self, input_archive, tag):
        input_archive.start_record(tag)
        self.czxid = input_archive.read_long('czxid')
        self.mzxid = input_archive.read_long('mzxid')
        self.ctime = input_archive.read_long('ctime')
        self.mtime = input_archive.read_long('mtime')
        self.version = input_archive.read_int('version')
        self.cversion = input_archive.read_int('cversion')
        self.aversion = input_archive.read_int('aversion')
        self.ephemeralOwner = input_archive.read_long('ephemeralOwner')
        self.dataLength = input_archive.read_int('dataLength')
        self.numChildren = input_archive.read_int('numChildren')
        self.pzxid = input_archive.read_long('pzxid')
        input_archive.end_record(tag)

    def __repr__(self):
        return 'Stat(%r, %r, %r, %r, %r, %r, %r, %r, %r, %r, %r)' % (self.czxid, self.mzxid, self.ctime, self.mtime, self.version, self.cversion, self.aversion, self.ephemeralOwner, self.dataLength, self.numChildren, self.pzxid)

    def __eq__(self, other):
        return self.czxid == other.czxid and self.mzxid == other.mzxid and self.ctime == other.ctime and self.mtime == other.mtime and self.version == other.version and self.cversion == other.cversion and self.aversion == other.aversion and self.ephemeralOwner == other.ephemeralOwner and self.dataLength == other.dataLength and self.numChildren == other.numChildren and self.pzxid == other.pzxid

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.czxid, self.mzxid, self.ctime, self.mtime, self.version, self.cversion, self.aversion, self.ephemeralOwner, self.dataLength, self.numChildren, self.pzxid))
