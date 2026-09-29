# Copyright the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import struct


class OutputArchive:
    def __init__(self):
        self.buffer = bytearray()

    def start_record(self, tag):
        pass

    def end_record(self, tag):
        pass

    def start_vector(self, v, tag):
        if v is None:
            self.write_int(-1, tag)
        else:
            self.write_int(len(v), tag)

    def end_vector(self, v, tag):
        pass

    def write_byte(self, b, tag):
        self.buffer.extend(struct.pack("B", b % 256))

    def write_bool(self, b, tag):
        self.buffer.extend([1 if b else 0])

    def write_int(self, i, tag):
        self.buffer.extend(struct.pack("!i", i))

    def write_long(self, value, tag):
        self.buffer.extend(struct.pack("!q", value))

    def write_string(self, s, tag):
        if not s:
            self.write_int(-1, "len")
        else:
            utf8_str = s.encode("utf-8")
            self.write_int(len(utf8_str), "len")
            self.buffer.extend(utf8_str)

    def write_buffer(self, buf, tag):
        if not buf:
            self.buffer.extend(struct.pack("!i", -1))
        else:
            self.buffer.extend(struct.pack("!i", len(buf)))
            self.buffer.extend(buf)

    def write_record(self, r, tag):
        r.serialize(self, tag)


class InputArchive:
    def __init__(self, buffer: bytearray):
        self.buffer = buffer
        self.offset = 0

    def start_record(self, tag: str):
        pass

    def end_record(self, tag: str):
        pass

    def start_vector(self, tag: str):
        length = self.read_int(tag)
        if length == -1:
            return None
        else:
            return length

    def end_vector(self, tag: str):
        pass

    def read_byte(self, tag: str):
        index = self.offset
        self.offset += 1
        return struct.unpack_from("B", self.buffer, index)[0]

    def read_bool(self, tag: str):
        return self.read_byte(tag) == 1

    def read_int(self, tag: str):
        index = self.offset
        self.offset += 4
        return struct.unpack_from("!i", self.buffer, index)[0]

    def read_long(self, tag: str):
        index = self.offset
        self.offset += 8
        return int(struct.unpack_from("!q", self.buffer, index)[0])

    def read_string(self, tag: str):
        length = self.read_int(tag)
        if length < 0:
            return None
        else:
            index = self.offset
            self.offset += length
            return str(self.buffer[index : index + length].decode("utf-8"))

    def read_buffer(self, tag: str):
        length = self.read_int(tag)
        if length < 0:
            return None
        else:
            index = self.offset
            self.offset += length
            return self.buffer[index : index + length]

    def read_record(self, r, tag: str):
        return r.deserialize(self, tag)
