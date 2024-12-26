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

import logging
from collections import defaultdict
from posixpath import split
from typing import NewType

from pookeeper.packets.data import Id
from pookeeper.packets.data.ACL import ACL

__version__ = "0.1.0-dev"

LOGGER = logging.getLogger(__name__)


def allocate(
        hosts,
        session_id=None,
        session_passwd: bytearray = None,
        session_timeout=30.0,
        auth_data=None,
        read_only=False,
        watcher=None,
        allow_reconnect=True,
):
    """Create a ZooKeeper client object

    To create a ZooKeeper client object, the application needs to pass a
    connection string containing a comma separated list of host:port pairs,
    each corresponding to a ZooKeeper server.

    Session establishment is asynchronous. This constructor will initiate
    connection to the server and return immediately - potentially (usually)
    before the session is fully established. The watcher argument specifies
    the watcher that will be notified of any changes in state. This
    notification can come at any point before or after the constructor call
    has returned.

    The instantiated ZooKeeper client object will pick an arbitrary server
    from the connectString and attempt to connect to it. If establishment of
    the connection fails, another server in the connect string will be tried
    (the order is non-deterministic, as we random shuffle the list), until a
    connection is established. The client will continue attempts until the
    session is explicitly closed (or the session is expired by the server).

    An optional "chroot" suffix may also be appended to the connection string.
    This will run the client commands while interpreting all paths relative to
    this root (similar to the unix chroot command).

    Use session_id and session_passwd on an established client connection,
    these values must be passed as session_id and session_passwd respectively
    if reconnecting. Otherwise, if not reconnecting, use the other constructor
    which does not require these parameters.

    Args:
        hosts: comma separated host:port pairs, each corresponding to a zk
            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
            If the optional chroot suffix is used the example would look
            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
            where the client would be rooted at "/app/a" and all paths
            would be relative to this root - ie getting/setting/etc...
            "/foo/bar" would result in operations being run on
            "/app/a/foo/bar" (from the server perspective).
        session_id: specific session id to use if reconnecting
        session_passwd: password for this session
        session_timeout: session timeout in milliseconds
        auth_data: a list of auth data for the connection
        read_only: whether the created client is allowed to go to
            read-only mode in case of partitioning. Read-only mode
            basically means that if the client can't find any majority
            servers but there's partitioned server it could reach, it
            connects to one in read-only mode, i.e. read requests are
            allowed while write requests are not. It continues seeking for
            majority in the background.
        watcher: a watcher object which will be notified of state changes, may
            also be notified for node events

    """
    return allocate_34(
        hosts, session_id, session_passwd, session_timeout, auth_data, read_only, watcher, allow_reconnect
    )


def allocate_34(
        hosts,
        session_id=None,
        session_passwd: bytearray = None,
        session_timeout=30.0,
        auth_data=None,
        read_only=False,
        watcher=None,
        allow_reconnect=True,
):
    """Create a ZooKeeper client object

    To create a ZooKeeper client object, the application needs to pass a
    connection string containing a comma separated list of host:port pairs,
    each corresponding to a ZooKeeper server.

    Session establishment is asynchronous. This constructor will initiate
    connection to the server and return immediately - potentially (usually)
    before the session is fully established. The watcher argument specifies
    the watcher that will be notified of any changes in state. This
    notification can come at any point before or after the constructor call
    has returned.

    The instantiated ZooKeeper client object will pick an arbitrary server
    from the connectString and attempt to connect to it. If establishment of
    the connection fails, another server in the connect string will be tried
    (the order is non-deterministic, as we random shuffle the list), until a
    connection is established. The client will continue attempts until the
    session is explicitly closed (or the session is expired by the server).

    An optional "chroot" suffix may also be appended to the connection string.
    This will run the client commands while interpreting all paths relative to
    this root (similar to the unix chroot command).

    Use session_id and session_passwd on an established client connection,
    these values must be passed as session_id and session_passwd respectively
    if reconnecting. Otherwise, if not reconnecting, use the other constructor
    which does not require these parameters.

    Args:
        hosts: comma separated host:port pairs, each corresponding to a zk
            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
            If the optional chroot suffix is used the example would look
            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
            where the client would be rooted at "/app/a" and all paths
            would be relative to this root - ie getting/setting/etc...
            "/foo/bar" would result in operations being run on
            "/app/a/foo/bar" (from the server perspective).
        session_id: specific session id to use if reconnecting
        session_passwd: password for this session
        session_timeout: session timeout in milliseconds
        auth_data: a list of auth data for the connection
        read_only: whether the created client is allowed to go to
            read-only mode in case of partitioning. Read-only mode
            basically means that if the client can't find any majority
            servers but there's partitioned server it could reach, it
            connects to one in read-only mode, i.e. read requests are
            allowed while write requests are not. It continues seeking for
            majority in the background.
        watcher: a watcher object which will be notified of state changes, may
            also be notified for node events

    """
    from pookeeper.zookeeper import Client34

    handle = Client34(
        hosts, session_id, session_passwd, session_timeout, auth_data, read_only, watcher, allow_reconnect
    )

    if LOGGER.isEnabledFor(logging.DEBUG):
        encoded_session_password = ''.join('{:02x}'.format(x) for x in session_passwd) if session_passwd else "None"
        LOGGER.debug(
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
        hosts,
        session_id=None,
        session_passwd: bytearray = None,
        session_timeout=30.0,
        auth_data=None,
        watcher=None,
        allow_reconnect=True,
):
    """Create a ZooKeeper client object

    To create a ZooKeeper client object, the application needs to pass a
    connection string containing a comma separated list of host:port pairs,
    each corresponding to a ZooKeeper server.

    Session establishment is asynchronous. This constructor will initiate
    connection to the server and return immediately - potentially (usually)
    before the session is fully established. The watcher argument specifies
    the watcher that will be notified of any changes in state. This
    notification can come at any point before or after the constructor call
    has returned.

    The instantiated ZooKeeper client object will pick an arbitrary server
    from the connectString and attempt to connect to it. If establishment of
    the connection fails, another server in the connect string will be tried
    (the order is non-deterministic, as we random shuffle the list), until a
    connection is established. The client will continue attempts until the
    session is explicitly closed (or the session is expired by the server).

    An optional "chroot" suffix may also be appended to the connection string.
    This will run the client commands while interpreting all paths relative to
    this root (similar to the unix chroot command).

    Use session_id and session_passwd on an established client connection,
    these values must be passed as session_id and session_passwd respectively
    if reconnecting. Otherwise, if not reconnecting, use the other constructor
    which does not require these parameters.

    Args:
        hosts: comma separated host:port pairs, each corresponding to a zk
            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
            If the optional chroot suffix is used the example would look
            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
            where the client would be rooted at "/app/a" and all paths
            would be relative to this root - ie getting/setting/etc...
            "/foo/bar" would result in operations being run on
            "/app/a/foo/bar" (from the server perspective).
        session_id: specific session id to use if reconnecting
        session_passwd: password for this session
        session_timeout: session timeout in milliseconds
        auth_data: a list of auth data for the connection
        watcher: a watcher object which will be notified of state changes, may
            also be notified for node events

    """
    from pookeeper.zookeeper import Client33

    handle = Client33(hosts, session_id, session_passwd, session_timeout, auth_data, watcher, allow_reconnect)

    if LOGGER.isEnabledFor(logging.DEBUG):
        encoded_session_password = ''.join('{:02x}'.format(x) for x in session_passwd) if session_passwd else "None"
        LOGGER.debug(
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


def delete(client, path):
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
    LOGGER.debug("Deleted %s", path)


def create(client, path, ACL=None, code=None):
    """Recursively create a path, creating intermediate nodes as required.

    Args:
        client: Pookeeper client
        path: the path to recursively create
        ACL: ACL to use for new node creation, default is CREATOR_ALL_ACL
        code: the type of the new nodes that are created, default is Persistent
    """
    if client.exists(path):
        return

    ACL = ACL or CREATOR_ALL_ACL
    code = code or Persistent()

    parent, node = split(path)

    if node:
        create(client, parent, ACL, code)
    try:
        client.create(path, ACL, code)
        LOGGER.debug("Created %s, ACL: %s, code %s", path, ACL, code)
    except NodeExistsError:
        pass


class Watcher:
    def session_connected(self, session_id, session_password: bytearray, read_only):
        pass

    def session_expired(self, session_id):
        pass

    def auth_failed(self):
        pass

    def connection_dropped(self):
        pass

    def connection_closed(self):
        pass

    def node_created(self, path):
        pass

    def node_deleted(self, path):
        pass

    def data_changed(self, path):
        pass

    def children_changed(self, path):
        pass


Key = NewType("Key", str)
WatchersDict = defaultdict[Key, set[Watcher]]


class State:
    def __init__(self, code, description):
        self.code = code
        self.description = description

    def __eq__(self, other):
        return self.code == other.code

    def __hash__(self):
        return hash(self.code)

    def __str__(self):
        return self.code

    def __repr__(self):
        return "%s()" % self.__class__.__name__


class Connecting(State):
    def __init__(self):
        super(Connecting, self).__init__("CONNECTING", "Connecting")


class Connected(State):
    def __init__(self):
        super(Connected, self).__init__("CONNECTED", "Connected")


class ConnectedRO(State):
    def __init__(self):
        super(ConnectedRO, self).__init__("CONNECTED_RO", "Connected Read-Only")


class AuthFailed(State):
    def __init__(self):
        super(AuthFailed, self).__init__("AUTH_FAILED", "Authorization Failed")


class Closed(State):
    def __init__(self):
        super(Closed, self).__init__("CLOSED", "Closed")


class ConnectionDroppedForTest(State):
    def __init__(self):
        super(ConnectionDroppedForTest, self).__init__(
            "CONNECTION_DROPPED_FOR_TEST", "Dropped connection for testing"
        )


CONNECTING = Connecting()
CONNECTED = Connected()
CONNECTED_RO = ConnectedRO()
AUTH_FAILED = AuthFailed()
CLOSED = Closed()
CONNECTION_DROPPED_FOR_TEST = ConnectionDroppedForTest()

CREATE_CODES = {}


class CreateCode:
    def __repr__(self):
        return "%s()" % self.__class__.__name__


def _create_code(name, flags, ephemeral, sequential):
    def decorator(klass):
        def attributes(self, name):
            if name == "ephemeral":
                return ephemeral
            if name == "sequential":
                return sequential
            if name == "flags":
                return flags
            raise AttributeError("Attribute %s not found" % name)

        klass.__getattr__ = attributes

        def string(self):
            return name

        klass.__str__ = string

        CREATE_CODES[flags] = klass()
        return klass

    return decorator


@_create_code("PERSISTENT", 0, False, False)
class Persistent(CreateCode):
    """
    The znode will not be automatically deleted upon client's disconnect.
    """

    pass


@_create_code("EPHEMERAL", 1, True, False)
class Ephemeral(CreateCode):
    """
    The znode will be deleted upon the client's disconnect.
    """

    pass


@_create_code("PERSISTENT_SEQUENTIAL", 2, False, True)
class PersistentSequential(CreateCode):
    """
    The znode will not be automatically deleted upon client's disconnect,
    and its name will be appended with a monotonically increasing number.
    """

    pass


@_create_code("EPHEMERAL_SEQUENTIAL", 3, True, True)
class EphemeralSequential(CreateCode):
    """
    The znode will be deleted upon the client's disconnect, and its name
    will be appended with a monotonically increasing number.

    """

    pass


class Perms:
    READ = 1
    WRITE = 2
    CREATE = 4
    DELETE = 8
    ADMIN = 16
    ALL = 31


OPEN_ACL_UNSAFE = [ACL(Perms.ALL, Id.ANYONE_ID_UNSAFE)]
CREATOR_ALL_ACL = [ACL(Perms.ALL, Id.AUTH_IDS)]
READ_ACL_UNSAFE = [ACL(Perms.READ, Id.ANYONE_ID_UNSAFE)]


def _invalid_error_code():
    raise RuntimeError("Invalid error code")


EXCEPTIONS = defaultdict(_invalid_error_code)


def _zookeeper_exception(code):
    def decorator(klass):
        def create(*args, **kwargs):
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
