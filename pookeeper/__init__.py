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

"""Pure Python ZooKeeper client.

Create a client with allocate() and use it as a context manager::

    import pookeeper

    with pookeeper.allocate("127.0.0.1:2181") as client:
        client.create("/app", data=bytearray(b"config"))
        data, stat = client.get_data("/app")
"""

from __future__ import annotations

import logging
from collections import defaultdict
from enum import IntEnum
from posixpath import split
from typing import TYPE_CHECKING, NoReturn, TypeVar

from pookeeper.packets.data.ACL import ACL
from pookeeper.packets.data.Id import Id

if TYPE_CHECKING:
    from collections.abc import Callable

    from pookeeper._typing import AuthData
    from pookeeper.zookeeper import Client33, Client34

__version__ = "0.1.0-dev"

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


def allocate(
    hosts: str,
    session_id: int | None = None,
    session_passwd: bytearray | None = None,
    session_timeout: float = 30.0,
    auth_data: AuthData | None = None,
    read_only: bool = False,
    watcher: Watcher | None = None,
    allow_reconnect: bool = True,
    default_acl: list[ACL] | None = None,
) -> Client34:
    """Create a ZooKeeper client object

    The hosts argument is a comma-separated list of host:port pairs, one for
    each ZooKeeper server.

    The client connects in the background and returns before the session is
    established. The watcher is notified when the session connects, which can
    happen before or after this call returns. Calls made before the session
    is established wait for it.

    The client tries the servers in a random order until one accepts the
    connection. If the connection drops, the client connects to the next
    server and keeps the same session. It keeps trying until the client is
    closed or the server expires the session.

    A path at the end of hosts sets a chroot. The client then runs all
    operations relative to that path, the way the Unix chroot command does.

    To resume an existing session, pass the session_id and session_passwd of
    a client that has connected. They are in its session.id and
    session.passwd attributes.

    Args:
        hosts: comma separated host:port pairs, each corresponding to a zk
            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
            If the optional chroot suffix is used the example would look
            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
            where the client would be rooted at "/app/a" and all paths
            would be relative to this root - ie getting/setting/etc...
            "/foo/bar" would result in operations being run on
            "/app/a/foo/bar" (from the server perspective).
        session_id: the ID of a session to resume
        session_passwd: the password of the session to resume
        session_timeout: the session timeout in seconds. The server can change
            it. The client's negotiated_session_timeout holds the value the
            server agreed to.
        auth_data: (scheme, credentials) pairs that the client authenticates
            with after each connect, for example [("digest", b"user:password")]
        read_only: whether the created client is allowed to go to
            read-only mode in case of partitioning. Read-only mode
            basically means that if the client can't find any majority
            servers but there's partitioned server it could reach, it
            connects to one in read-only mode, i.e. read requests are
            allowed while write requests are not. It continues seeking for
            majority in the background.
        watcher: a watcher object which will be notified of state changes, may
            also be notified for node events
        allow_reconnect: whether to reconnect after the connection drops. If
            False, the client closes when its first connection is lost.
        default_acl: the ACL that create() uses when it is given no ACL. The
            default is OPEN_ACL_UNSAFE.

    """
    return allocate_34(
        hosts,
        session_id,
        session_passwd,
        session_timeout,
        auth_data,
        read_only,
        watcher,
        allow_reconnect,
        default_acl,
    )


def allocate_34(
    hosts: str,
    session_id: int | None = None,
    session_passwd: bytearray | None = None,
    session_timeout: float = 30.0,
    auth_data: AuthData | None = None,
    read_only: bool = False,
    watcher: Watcher | None = None,
    allow_reconnect: bool = True,
    default_acl: list[ACL] | None = None,
) -> Client34:
    """Create a ZooKeeper client object

    The hosts argument is a comma-separated list of host:port pairs, one for
    each ZooKeeper server.

    The client connects in the background and returns before the session is
    established. The watcher is notified when the session connects, which can
    happen before or after this call returns. Calls made before the session
    is established wait for it.

    The client tries the servers in a random order until one accepts the
    connection. If the connection drops, the client connects to the next
    server and keeps the same session. It keeps trying until the client is
    closed or the server expires the session.

    A path at the end of hosts sets a chroot. The client then runs all
    operations relative to that path, the way the Unix chroot command does.

    To resume an existing session, pass the session_id and session_passwd of
    a client that has connected. They are in its session.id and
    session.passwd attributes.

    Args:
        hosts: comma separated host:port pairs, each corresponding to a zk
            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
            If the optional chroot suffix is used the example would look
            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
            where the client would be rooted at "/app/a" and all paths
            would be relative to this root - ie getting/setting/etc...
            "/foo/bar" would result in operations being run on
            "/app/a/foo/bar" (from the server perspective).
        session_id: the ID of a session to resume
        session_passwd: the password of the session to resume
        session_timeout: the session timeout in seconds. The server can change
            it. The client's negotiated_session_timeout holds the value the
            server agreed to.
        auth_data: (scheme, credentials) pairs that the client authenticates
            with after each connect, for example [("digest", b"user:password")]
        read_only: whether the created client is allowed to go to
            read-only mode in case of partitioning. Read-only mode
            basically means that if the client can't find any majority
            servers but there's partitioned server it could reach, it
            connects to one in read-only mode, i.e. read requests are
            allowed while write requests are not. It continues seeking for
            majority in the background.
        watcher: a watcher object which will be notified of state changes, may
            also be notified for node events
        allow_reconnect: whether to reconnect after the connection drops. If
            False, the client closes when its first connection is lost.
        default_acl: the ACL that create() uses when it is given no ACL. The
            default is OPEN_ACL_UNSAFE.

    """
    from pookeeper.zookeeper import Client34

    handle = Client34(
        hosts,
        session_id,
        session_passwd,
        session_timeout,
        auth_data,
        read_only,
        watcher,
        allow_reconnect,
        default_acl,
    )

    if _logger.isEnabledFor(logging.DEBUG):
        encoded_session_password = (
            "".join(f"{x:02x}" for x in session_passwd) if session_passwd else "None"
        )
        _logger.debug(
            "Allocated v3.4 client, %s, %s, 0x%s, %s, %r, %s, %s, %s",
            hosts,
            session_id,
            encoded_session_password,
            session_timeout,
            auth_data,
            read_only,
            watcher,
            allow_reconnect,
        )

    return handle


def allocate_33(
    hosts: str,
    session_id: int | None = None,
    session_passwd: bytearray | None = None,
    session_timeout: float = 30.0,
    auth_data: AuthData | None = None,
    watcher: Watcher | None = None,
    allow_reconnect: bool = True,
    default_acl: list[ACL] | None = None,
) -> Client33:
    """Create a ZooKeeper client object

    The hosts argument is a comma-separated list of host:port pairs, one for
    each ZooKeeper server.

    The client connects in the background and returns before the session is
    established. The watcher is notified when the session connects, which can
    happen before or after this call returns. Calls made before the session
    is established wait for it.

    The client tries the servers in a random order until one accepts the
    connection. If the connection drops, the client connects to the next
    server and keeps the same session. It keeps trying until the client is
    closed or the server expires the session.

    A path at the end of hosts sets a chroot. The client then runs all
    operations relative to that path, the way the Unix chroot command does.

    To resume an existing session, pass the session_id and session_passwd of
    a client that has connected. They are in its session.id and
    session.passwd attributes.

    Args:
        hosts: comma separated host:port pairs, each corresponding to a zk
            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
            If the optional chroot suffix is used the example would look
            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
            where the client would be rooted at "/app/a" and all paths
            would be relative to this root - ie getting/setting/etc...
            "/foo/bar" would result in operations being run on
            "/app/a/foo/bar" (from the server perspective).
        session_id: the ID of a session to resume
        session_passwd: the password of the session to resume
        session_timeout: the session timeout in seconds. The server can change
            it. The client's negotiated_session_timeout holds the value the
            server agreed to.
        auth_data: (scheme, credentials) pairs that the client authenticates
            with after each connect, for example [("digest", b"user:password")]
        watcher: a watcher object which will be notified of state changes, may
            also be notified for node events
        allow_reconnect: whether to reconnect after the connection drops. If
            False, the client closes when its first connection is lost.
        default_acl: the ACL that create() uses when it is given no ACL. The
            default is OPEN_ACL_UNSAFE.

    """
    from pookeeper.zookeeper import Client33

    handle = Client33(
        hosts,
        session_id,
        session_passwd,
        session_timeout,
        auth_data,
        watcher,
        allow_reconnect,
        default_acl,
    )

    if _logger.isEnabledFor(logging.DEBUG):
        encoded_session_password = (
            "".join(f"{x:02x}" for x in session_passwd) if session_passwd else "None"
        )
        _logger.debug(
            "Allocated v3.3 client, %s, %s, 0x%s, %s, %r, %s, %s",
            hosts,
            session_id,
            encoded_session_password,
            session_timeout,
            auth_data,
            watcher,
            allow_reconnect,
        )

    return handle


def delete(client: Client33, path: str) -> None:
    """Recursively delete a path

    Args:
        client: Pookeeper client
        path: the path to recursively delete
    """
    if not client.exists(path):
        return

    children, stat = client.get_children(path)
    for child in children:
        delete(client, path + "/" + child)
    client.delete(path, stat.version)
    _logger.debug("Deleted %s", path)


def create(
    client: Client33,
    path: str,
    ACL: list[ACL] | None = None,
    code: CreateCode | None = None,
) -> None:
    """Recursively create a path, creating intermediate nodes as required.

    Args:
        client: Pookeeper client
        path: the path to recursively create
        ACL: ACL to use for new node creation. The default is the client's
            default_acl.
        code: the type of the new nodes that are created, default is Persistent
    """
    if client.exists(path):
        return

    code = code or Persistent()

    parent, node = split(path)

    if node:
        create(client, parent, ACL, code)
    try:
        client.create(path, ACL, code)
        _logger.debug("Created %s, ACL: %s, code %s", path, ACL, code)
    except NodeExistsError:
        pass


class WatcherEventType(IntEnum):
    CREATED_EVENT = 1
    DELETE_EVENT = 2
    DATA_CHANGED_EVENT = 3
    CHILD_CHANGED_EVENT = 4


class Watcher:
    """Receives session and node events from a client.

    Subclass it and override the callbacks you need. The base methods do
    nothing. Pass a watcher to allocate() to receive session events and the
    node events of watches set with watch=True. Pass one as the watcher
    argument of exists(), get_data(), or get_children() to receive the node
    events of that watch only.

    All callbacks run in order on the client's event thread. A callback that
    blocks delays the events after it. The client logs and ignores exceptions
    that a callback raises.

    A node watch fires once. Set it again to receive the next event. The path
    that node callbacks receive is the full path on the server, including the
    client's chroot.
    """

    def session_connected(
        self, session_id: int, session_password: bytearray, read_only: bool
    ) -> None:
        """Called each time the client connects, including after a reconnect.

        Args:
            session_id: the ID of the session
            session_password: the password of the session
            read_only: whether the client is connected to a read-only server
        """

    def session_expired(self, session_id: int | None) -> None:
        """Called when the server reports that the session has expired.

        The client closes. Create a new client to continue.

        Args:
            session_id: the ID of the expired session
        """

    def auth_failed(self) -> None:
        """Called when the server rejects the client's auth_data.

        The client closes.
        """

    def connection_dropped(self) -> None:
        """Called when the connection to a server drops.

        Calls in progress raise ConnectionLoss. The client then reconnects and
        keeps the session, unless allow_reconnect is False.
        """

    def connection_closed(self) -> None:
        """Called when the client closes."""

    def node_created(self, path: str) -> None:
        """Called when a watched node is created.

        Args:
            path: the full path of the node on the server
        """

    def node_deleted(self, path: str) -> None:
        """Called when a watched node is deleted.

        Args:
            path: the full path of the node on the server
        """

    def data_changed(self, path: str) -> None:
        """Called when the data of a watched node is set.

        Args:
            path: the full path of the node on the server
        """

    def children_changed(self, path: str) -> None:
        """Called when a child is added to or removed from a watched node.

        Args:
            path: the full path of the node on the server
        """


WatchersDict = defaultdict[str, set[Watcher]]


class State:
    """A connection state of a client, available as client.state.

    Compare it with these module constants:

    - CONNECTING: the client is connecting or reconnecting.
    - CONNECTED: the client is connected and has a session.
    - CONNECTED_RO: the client is connected to a read-only server. Writes
      fail.
    - AUTH_FAILED: the server rejected auth_data. The client is closed.
    - CLOSED: the client is closed, or the server expired the session.
    - CONNECTION_DROPPED_FOR_TEST: the connection dropped while
      allow_reconnect was False. The client is closed.

    Attributes:
        code: the name of the state, for example "CONNECTED"
        description: a readable description of the state
    """

    def __init__(self, code: str, description: str) -> None:
        self.code = code
        self.description = description

    def __eq__(self, other: object) -> bool:
        return isinstance(other, State) and self.code == other.code

    def __hash__(self) -> int:
        return hash(self.code)

    def __str__(self) -> str:
        return self.code

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


class Connecting(State):
    def __init__(self) -> None:
        super().__init__("CONNECTING", "Connecting")


class Connected(State):
    def __init__(self) -> None:
        super().__init__("CONNECTED", "Connected")


class ConnectedRO(State):
    def __init__(self) -> None:
        super().__init__("CONNECTED_RO", "Connected Read-Only")


class AuthFailed(State):
    def __init__(self) -> None:
        super().__init__("AUTH_FAILED", "Authorization Failed")


class Closed(State):
    def __init__(self) -> None:
        super().__init__("CLOSED", "Closed")


class ConnectionDroppedForTest(State):
    def __init__(self) -> None:
        super().__init__(
            "CONNECTION_DROPPED_FOR_TEST", "Dropped connection for testing"
        )


CONNECTING = Connecting()
CONNECTED = Connected()
CONNECTED_RO = ConnectedRO()
AUTH_FAILED = AuthFailed()
CLOSED = Closed()
CONNECTION_DROPPED_FOR_TEST = ConnectionDroppedForTest()

CREATE_CODES: dict[int, CreateCode] = {}
"""Maps each ZooKeeper create flag to a CreateCode instance."""


class CreateCode:
    """The kind of node that create() makes.

    Use an instance of Persistent, Ephemeral, PersistentSequential, or
    EphemeralSequential.

    Attributes:
        flags: the ZooKeeper create flag
        ephemeral: whether the server deletes the node when the session that
            created it ends
        sequential: whether the server appends a sequence number to the name
    """

    flags: int
    ephemeral: bool
    sequential: bool

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


_C = TypeVar("_C", bound=CreateCode)


def _create_code(
    name: str, flags: int, ephemeral: bool, sequential: bool
) -> Callable[[type[_C]], type[_C]]:
    def decorator(klass: type[_C]) -> type[_C]:
        def attributes(self: CreateCode, name: str) -> bool | int:
            if name == "ephemeral":
                return ephemeral
            if name == "sequential":
                return sequential
            if name == "flags":
                return flags
            raise AttributeError(f"Attribute {name} not found")

        setattr(klass, "__getattr__", attributes)  # noqa: B010

        def string(self: CreateCode) -> str:
            return name

        setattr(klass, "__str__", string)  # noqa: B010

        CREATE_CODES[flags] = klass()
        return klass

    return decorator


@_create_code("PERSISTENT", 0, False, False)
class Persistent(CreateCode):
    """The node stays until it is deleted."""

    pass


@_create_code("EPHEMERAL", 1, True, False)
class Ephemeral(CreateCode):
    """The server deletes the node when the session that created it ends."""

    pass


@_create_code("PERSISTENT_SEQUENTIAL", 2, False, True)
class PersistentSequential(CreateCode):
    """The node stays until it is deleted.

    The server appends an increasing sequence number to its name.
    """

    pass


@_create_code("EPHEMERAL_SEQUENTIAL", 3, True, True)
class EphemeralSequential(CreateCode):
    """The server deletes the node when the session that created it ends.

    The server appends an increasing sequence number to its name.
    """

    pass


class Perms:
    """ACL permission bits.

    Combine them with |, for example Perms.READ | Perms.WRITE.

    Attributes:
        READ: get the data of the node and list its children
        WRITE: set the data of the node
        CREATE: create children of the node
        DELETE: delete children of the node
        ADMIN: set the ACL of the node
        ALL: all of the above
    """

    READ = 1
    WRITE = 2
    CREATE = 4
    DELETE = 8
    ADMIN = 16
    ALL = 31


ANYONE_ID_UNSAFE = Id("world", "anyone")
"""The identity that matches every client."""

AUTH_IDS = Id("auth", "")
"""The identities the creating session has authenticated with."""

OPEN_ACL_UNSAFE = [ACL(Perms.ALL, ANYONE_ID_UNSAFE)]
"""Gives every client full access to the node."""

CREATOR_ALL_ACL = [ACL(Perms.ALL, AUTH_IDS)]
"""Gives full access only to the identities the creating session authenticated
with. Creating a node with it fails if the session has not authenticated."""

READ_ACL_UNSAFE = [ACL(Perms.READ, ANYONE_ID_UNSAFE)]
"""Lets every client read the node."""


def _invalid_error_code() -> NoReturn:
    raise RuntimeError("Invalid error code")


EXCEPTIONS: defaultdict[int, Callable[..., ZookeeperError]] = defaultdict(
    _invalid_error_code
)
"""Maps each ZooKeeper error code to a factory for its ZookeeperError."""


_E = TypeVar("_E", bound="ZookeeperError")


def _zookeeper_exception(code: int) -> Callable[[type[_E]], type[_E]]:
    def decorator(klass: type[_E]) -> type[_E]:
        def create(*args: object, **kwargs: object) -> _E:
            return klass(args, kwargs)

        EXCEPTIONS[code] = create
        return klass

    return decorator


class ZookeeperError(RuntimeError):
    """Parent exception for all zookeeper errors"""

    pass


@_zookeeper_exception(0)
class RolledBackError(ZookeeperError):
    pass


@_zookeeper_exception(-1)
class SystemZookeeperError(ZookeeperError):
    pass


@_zookeeper_exception(-2)
class RuntimeInconsistency(ZookeeperError):
    pass


@_zookeeper_exception(-3)
class DataInconsistency(ZookeeperError):
    pass


@_zookeeper_exception(-4)
class ConnectionLoss(ZookeeperError):
    pass


@_zookeeper_exception(-5)
class MarshallingError(ZookeeperError):
    pass


@_zookeeper_exception(-6)
class UnimplementedError(ZookeeperError):
    pass


@_zookeeper_exception(-7)
class OperationTimeoutError(ZookeeperError):
    pass


@_zookeeper_exception(-8)
class BadArgumentsError(ZookeeperError):
    pass


@_zookeeper_exception(-100)
class APIError(ZookeeperError):
    pass


@_zookeeper_exception(-101)
class NoNodeError(ZookeeperError):
    pass


@_zookeeper_exception(-102)
class NoAuthError(ZookeeperError):
    pass


@_zookeeper_exception(-103)
class BadVersionError(ZookeeperError):
    pass


@_zookeeper_exception(-108)
class NoChildrenForEphemeralsError(ZookeeperError):
    pass


@_zookeeper_exception(-110)
class NodeExistsError(ZookeeperError):
    pass


@_zookeeper_exception(-111)
class NotEmptyError(ZookeeperError):
    pass


@_zookeeper_exception(-112)
class SessionExpiredError(ZookeeperError):
    pass


@_zookeeper_exception(-113)
class InvalidCallbackError(ZookeeperError):
    pass


@_zookeeper_exception(-114)
class InvalidACLError(ZookeeperError):
    pass


@_zookeeper_exception(-115)
class AuthFailedError(ZookeeperError):
    pass
