"""
 Copyright 2012 the original author or authors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
"""
from toolazydogs.zookeeper.archive import OutputArchive, InputArchive
from toolazydogs.zookeeper.packets.proto.ConnectRequest import ConnectRequest


def test_byte():
    oa = OutputArchive()
    for b in range(256):
        oa.write_byte(b, 'tag_' + str(b))
    ia = InputArchive(str(oa.buffer))
    for b in range(256):
        r = ia.read_byte('tag_' + str(b))
        assert b == r


def test_bool():
    oa = OutputArchive()
    oa.write_bool(True, 'tag')
    ia = InputArchive(str(oa.buffer))
    assert ia.read_bool('tag')

    oa = OutputArchive()
    oa.write_bool(False, 'tag')
    ia = InputArchive(str(oa.buffer))
    assert not ia.read_bool('tag')

def test_int():
    oa = OutputArchive()
    for b in range(256):
        oa.write_int(b, 'tag_' + str(b))
    ia = InputArchive(str(oa.buffer))
    for b in range(256):
        r = ia.read_int('tag_' + str(b))
        assert b == r

#def test_output_archive():
#    oa = OutputArchive()
#    connect_request = ConnectRequest.allocate(-1, 3, 4, 2, 'secret', True)
#    connect_request.serialize(oa, 'Foo')
#
#    ia = InputArchive(str(oa.buffer))
#
#    test = ConnectRequest()
#    test.deserialize(ia, 'Bar')
#
#    print 'connect_request %r' % connect_request
#    print 'test %r' % test
#
#    assert connect_request == test
