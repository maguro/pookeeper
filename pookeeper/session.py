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


class Session:
    id: int | None
    last_zxid: int
    passwd: bytearray

    def __init__(
        self,
        session_id: int | None = None,
        last_zxid: int = 0,
        session_passwd: bytearray | None = None,
    ) -> None:
        self.id = session_id
        self.last_zxid = last_zxid
        self.passwd = session_passwd if session_passwd else bytearray([0] * 16)

    def __repr__(self):
        return f"Session(id={self.id}, last_zxid={self.last_zxid})"
