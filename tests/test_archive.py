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

from pookeeper.archive import InputArchive, OutputArchive
from pookeeper.packets.proto.ConnectRequest import ConnectRequest


def test_byte():
    oa = OutputArchive()
    for b in range(256):
        oa.write_byte(b, "tag_" + str(b))
    ia = InputArchive(oa.buffer)
    for b in range(256):
        r = ia.read_byte("tag_" + str(b))
        assert b == r


def test_bool():
    oa = OutputArchive()
    oa.write_bool(True, "tag")
    ia = InputArchive(oa.buffer)
    assert ia.read_bool("tag")

    oa = OutputArchive()
    oa.write_bool(False, "tag")
    ia = InputArchive(oa.buffer)
    assert not ia.read_bool("tag")


def test_int():
    oa = OutputArchive()
    for b in range(256):
        oa.write_int(b, "tag_" + str(b))
    ia = InputArchive(oa.buffer)
    for b in range(256):
        r = ia.read_int("tag_" + str(b))
        assert type(r) == int
        assert b == r


def test_long():
    oa = OutputArchive()
    oa.write_long(9141385893744296737, "tag")
    ia = InputArchive(oa.buffer)
    r = ia.read_long("tag")
    assert isinstance(r, int)
    assert r == 9141385893744296737

    oa = OutputArchive()
    oa.write_long(1, "tag")
    ia = InputArchive(oa.buffer)
    r = ia.read_long("tag")
    assert isinstance(r, int)
    assert r == 1


def test_vector():
    oa = OutputArchive()
    v = [1, 2, 3, 4, 5]
    oa.start_vector(v, "tag")
    for i in range(len(v)):
        oa.write_int(v[i], "tag_" + str(i))
    oa.end_vector(v, "tag")

    ia = InputArchive(oa.buffer)
    l = ia.start_vector("tag")
    vv = []
    for i in range(l):
        vv.append(ia.read_int("tag_" + str(i)))
    ia.end_vector("tag")
    assert v == vv


def test_connect_request():
    oa = OutputArchive()
    original = ConnectRequest(1, 3, 10, 123456, bytearray("secret", encoding="UTF-8"), True)
    original.serialize(oa, "Foo")

    ia = InputArchive(oa.buffer)

    test = ConnectRequest(0, 0, 0, 0, None, False)
    test.deserialize(ia, "Bar")

    assert original == test
