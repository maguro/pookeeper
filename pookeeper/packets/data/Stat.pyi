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

from pookeeper.archive import InputArchive, OutputArchive

class Stat:
    czxid: int
    mzxid: int
    ctime: int
    mtime: int
    version: int
    cversion: int
    aversion: int
    ephemeralOwner: int
    dataLength: int
    numChildren: int
    pzxid: int

    def __init__(
        self,
        czxid: int | None,
        mzxid: int | None,
        ctime: int | None,
        mtime: int | None,
        version: int | None,
        cversion: int | None,
        aversion: int | None,
        ephemeralOwner: int | None,
        dataLength: int | None,
        numChildren: int | None,
        pzxid: int | None,
    ) -> None: ...
    def serialize(self, output_archive: OutputArchive, tag: str) -> None: ...
    def deserialize(self, input_archive: InputArchive, tag: str) -> None: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
