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
from pookeeper.packets.data.ACL import ACL
from pookeeper.packets.data.Stat import Stat


class GetACLResponse:
    def __init__(self, acl, stat):
        self.acl = acl
        self.stat = stat

    def serialize(self, output_archive, tag):
        output_archive.start_record(tag)
        output_archive.start_vector(self.acl, 'acl')
        if self.acl != None:
            for e1 in self.acl:
                output_archive.write_record(e1, 'e1')
        output_archive.end_vector(self.acl, 'acl')
        output_archive.write_record(self.stat, 'stat')
        output_archive.end_record(tag)

    def deserialize(self, input_archive, tag):
        input_archive.start_record(tag)
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
        self.stat = Stat(None, None, None, None, None, None, None, None, None, None, None)
        input_archive.read_record(self.stat, 'stat')
        input_archive.end_record(tag)

    def __repr__(self):
        return 'GetACLResponse(%r, %r)' % (self.acl, self.stat)

    def __eq__(self, other):
        return self.acl == other.acl and self.stat == other.stat

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.acl, self.stat))
