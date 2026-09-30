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

"""Type aliases and protocols for static type checking.

Import these names only under ``if TYPE_CHECKING:``.
"""

from __future__ import annotations

from collections.abc import Callable, Collection
from typing import TYPE_CHECKING, Protocol, TypeAlias

if TYPE_CHECKING:
    import sys

    if sys.version_info >= (3, 11):
        from typing import Self as Self
    else:
        from typing_extensions import Self as Self

    from pookeeper import ZookeeperError
    from pookeeper.archive import InputArchive, OutputArchive
    from pookeeper.packets.data.Stat import Stat


class Serializable(Protocol):
    """A Jute record that can write itself to an archive."""

    def serialize(self, output_archive: OutputArchive, tag: str) -> None: ...


class Deserializable(Protocol):
    """A Jute record that can read itself from an archive."""

    def deserialize(self, input_archive: InputArchive, tag: str) -> None: ...


class Request(Serializable, Protocol):
    """A Jute request record with its ZooKeeper operation code."""

    @property
    def type(self) -> int | None: ...


AuthData: TypeAlias = Collection[tuple[str, bytes | bytearray]]
Callback: TypeAlias = "Callable[[ZookeeperError | None], None]"
QueuedCall: TypeAlias = "tuple[Request, Deserializable | None, Callback]"
PendingCall: TypeAlias = "tuple[Request, Deserializable | None, Callback, int]"
TransactionResult: TypeAlias = "str | Stat | tuple[()] | ZookeeperError"
WatcherRegistration: TypeAlias = "Callable[[ZookeeperError | None], object]"
