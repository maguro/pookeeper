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
from toolazydogs.zookeeper.packets.data.ACL import ACL


class SetACLRequest:
    def __init__(self, path, acl, version):
        self.type = 7
        self.path = path
        self.acl = acl
        self.version = version

    def serialize(self, output_archive, tag):
        output_archive.start_record(tag)
        output_archive.write_string(self.path, 'path')
        output_archive.start_vector(self.acl, 'acl')
        if self.acl != None:
            for e1 in self.acl:
                output_archive.write_record(e1, 'e1')
        output_archive.end_vector(self.acl, 'acl')
        output_archive.write_int(self.version, 'version')
        output_archive.end_record(tag)

    def deserialize(self, input_archive, tag):
        input_archive.start_record(tag)
        self.path = input_archive.read_string('path')
        len1 = input_archive.start_vector('acl')
        if len1 != None:
            self.acl = []
            for vidx1 in range(len1):
                e1 = ACL(None, None)
                input_archive.read_record(e1, 'e1')
                self.acl.append(e1)
        else:
            self.acl = None
        input_archive.end_vector('acl')
        self.version = input_archive.read_int('version')
        input_archive.end_record(tag)

    def __repr__(self):
        return 'SetACLRequest(%r, %r, %r)' % (self.path, self.acl, self.version)

    def __eq__(self, other):
        return self.path == other.path and self.acl == other.acl and self.version == other.version

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.path, self.acl, self.version))
