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
import socket
import threading
from collections import defaultdict
from queue import Queue
from typing import List, Optional, Tuple

from pookeeper import (
    AUTH_FAILED,
    AuthFailedError,
    CLOSED,
    CONNECTED,
    CONNECTED_RO,
    CONNECTING,
    CONNECTION_DROPPED_FOR_TEST,
    ConnectionLoss,
    CreateCode,
    InvalidACLError,
    NoNodeError,
    SessionExpiredError,
    Watcher,
    WatchersDict,
    events,
    zkpath,
)
from pookeeper.hosts import collect_hosts
from pookeeper.impl import ConnectionDroppedForTest, PeekableQueue, WriterThread
from pookeeper.packets.data.ACL import ACL
from pookeeper.packets.data.Stat import Stat
from pookeeper.packets.proto.CheckVersionRequest import CheckVersionRequest
from pookeeper.packets.proto.CloseRequest import CloseRequest
from pookeeper.packets.proto.CloseResponse import CloseResponse
from pookeeper.packets.proto.CreateRequest import CreateRequest
from pookeeper.packets.proto.CreateResponse import CreateResponse
from pookeeper.packets.proto.DeleteRequest import DeleteRequest
from pookeeper.packets.proto.ExistsRequest import ExistsRequest
from pookeeper.packets.proto.ExistsResponse import ExistsResponse
from pookeeper.packets.proto.GetACLRequest import GetACLRequest
from pookeeper.packets.proto.GetACLResponse import GetACLResponse
from pookeeper.packets.proto.GetChildren2Request import GetChildren2Request
from pookeeper.packets.proto.GetChildren2Response import GetChildren2Response
from pookeeper.packets.proto.GetDataRequest import GetDataRequest
from pookeeper.packets.proto.GetDataResponse import GetDataResponse
from pookeeper.packets.proto.SetACLRequest import SetACLRequest
from pookeeper.packets.proto.SetACLResponse import SetACLResponse
from pookeeper.packets.proto.SetDataRequest import SetDataRequest
from pookeeper.packets.proto.SetDataResponse import SetDataResponse
from pookeeper.packets.proto.SyncRequest import SyncRequest
from pookeeper.packets.proto.SyncResponse import SyncResponse
from pookeeper.packets.proto.TransactionRequest import TransactionRequest
from pookeeper.packets.proto.TransactionResponse import TransactionResponse

LOGGER = logging.getLogger(__name__)

_ID = 0
_ID_LOCK = threading.RLock()


def log_wrapper():
    """A class method decorator that renames the current thread to identify the current pookeeper client"""

    def wrapper(method):
        def new(self, *args, **kws):
            global _ID
            try:
                name = "pookeeper-%s" % self.id
            except AttributeError:
                with _ID_LOCK:
                    self.id = _ID
                    _ID += 1
                name = "pookeeper-%s" % self.id
            current_thread = threading.current_thread()
            current_name = current_thread.name
            current_thread.name = name if current_name == "MainThread" else current_name
            try:
                return method(self, *args, **kws)
            finally:
                current_thread.name = current_name

        return new

    return wrapper


class Client33:
    id: int

    @log_wrapper()
    def __init__(
            self,
            hosts,
            session_id=None,
            session_passwd: bytearray = None,
            session_timeout=30.0,
            auth_data=None,
            watcher: Watcher = None,
            allow_reconnect=True,
    ):
        self.hosts, chroot = collect_hosts(hosts)
        if chroot:
            self.chroot = zkpath.normpath(chroot)
            if not zkpath.isabs(self.chroot):
                raise ValueError("chroot not absolute")
        else:
            self.chroot = ""

        self.session_id = session_id
        self.session_passwd = session_passwd if session_passwd else bytearray([0] * 16)
        self.session_timeout = session_timeout
        self.connect_timeout = session_timeout / len(self.hosts)
        self.read_timeout = session_timeout * 2.0 / 3.0
        self.auth_data = auth_data if auth_data else set([])
        self.read_only = False

        if LOGGER.isEnabledFor(logging.DEBUG):
            encoded_session_password = ''.join('{:02x}'.format(x) for x in session_passwd) if session_passwd else "None"

            LOGGER.debug("session_id: %s", self.session_id)
            LOGGER.debug("session_passwd: 0x%s", encoded_session_password)
            LOGGER.debug("session_timeout: %s", self.session_timeout)
            LOGGER.debug("connect_timeout: %s", self.connect_timeout)
            LOGGER.debug("   len(hosts): %s", len(self.hosts))
            LOGGER.debug("read_timeout: %s", self.read_timeout)
            LOGGER.debug("auth_data: %s", self.auth_data)

        self.allow_reconnect = allow_reconnect
        LOGGER.debug("allow_reconnect: %s", self.allow_reconnect)

        self.last_zxid = 0

        self._queue = PeekableQueue()
        self._pending = Queue()

        self._child_watchers: WatchersDict = defaultdict(set)
        self._data_watchers: WatchersDict = defaultdict(set)
        self._exists_watchers: WatchersDict = defaultdict(set)
        self._default_watcher: Watcher = watcher or Watcher()

        self.state = CONNECTING
        self._state_lock = threading.RLock()

        self._events = events.Events(self.id)
        self._events.start()

        self._writer_thread = WriterThread(self)
        self._writer_thread.daemon = True
        self._writer_thread.start()

        self._check_state()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @log_wrapper()
    def close(self):
        """Close this client object

        Once the client is closed, its session becomes invalid. All the
        ephemeral nodes in the ZooKeeper server associated with the session
        will be removed. The watches left on those nodes (and on their parents)
        will be triggered.
        """

        LOGGER.debug("close()")

        call_exception: Optional[BaseException] = None

        with self._state_lock:
            if self.state == AUTH_FAILED:
                return
            if self.state == CLOSED:
                return

            def close(exception):
                nonlocal call_exception
                call_exception = exception
                LOGGER.debug("Closing handler called")

            self._queue.put((CloseRequest(), CloseResponse(), close))

        # the events queue will be stopped when the writer thread closes
        self._events.join()

        self.session_id = None
        self.session_passwd = bytearray([0] * 16)

        if call_exception:
            raise call_exception

    @log_wrapper()
    def create(self, path: str, acls: List[ACL], code: CreateCode, data: bytearray = None) -> str:
        """Create a node with the given path

        The node data will be the given data, and node acl will be the given
        acl.

        The code argument specifies whether the created node will be ephemeral
        or not.

        An ephemeral node will be removed by the ZooKeeper automatically when the
        session associated with the creation of the node expires.

        The code argument can also specify to create a sequential node. The
        actual path name of a sequential node will be the given path plus a
        suffix "i" where i is the current sequential number of the node. The sequence
        number is always fixed length of 10 digits, 0 padded. Once
        such a node is created, the sequential number will be incremented by one.

        If a node with the same actual path already exists in the ZooKeeper, a
        NodeExistsError will be raised. Note that since a different actual path
        is used for each invocation of creating sequential node with the same
        path argument, the call will never raise NodeExistsError.

        If the parent node does not exist in the ZooKeeper, a NoNodeError will
        be raised.

        An ephemeral node cannot have children. If the parent node of the given
        path is ephemeral, a NoChildrenForEphemeralsError will be raised.

        This operation, if successful, will trigger all the watches left on the
        node of the given path by exists and get_data() API calls, and the watches
        left on the parent node by get_children() API calls.

        If a node is created successfully, the ZooKeeper server will trigger the
        watches on the path left by exists calls, and the watches on the parent
        of the node by getChildren calls.

        The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
        Arrays larger than this will cause a ZookeeperError to be raised.

        Args:
            path: the path for the node
            acls: the acl for the node
            code: specifying whether the node to be created is ephemeral
                and/or sequential
            data: optional initial data for the node

        Returns:
            the actual path of the created node

        Raises:
            ZookeeperError: if the server returns a non-zero error code
            InvalidACLError: if the ACL is invalid, null, or empty
            ValueError: if an invalid path is specified

        """

        LOGGER.debug("create(%r, %r, %r, %r)", path, acls, code, data)

        if not acls:
            raise InvalidACLError("ACLs cannot be None or empty")
        if not code:
            raise ValueError("Creation code cannot be None")

        request = CreateRequest(_prefix_root(self.chroot, path), data, acls, code.flags)
        response = CreateResponse(None)

        self._call(request, response)

        return response.path[len(self.chroot):]

    @log_wrapper()
    def delete(self, path: str, version: int = -1) -> None:
        """Delete the node with the given path

        The call will succeed if such a node exists, and the given version
        matches the node's version (if the given version is -1, the default,
        it matches any node's versions).

        A NodeExistsError will be raised if the nodes does not exist.

        A BadVersionError will be raised if the given version does not match
        the node's version.

        A NotEmptyError will be raised if the node has children.

        This operation, if successful, will trigger all the watches on the node
        of the given path left by exists API calls, and the watches on the parent
        node left by get_children() API calls.

        Args:
            path: the path of the node to be deleted.
            version: the expected node version

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """

        LOGGER.debug("delete(%r, %r)", path, version)

        request = DeleteRequest(_prefix_root(self.chroot, path), version)

        self._call(request, None)

    @log_wrapper()
    def exists(self, path: str, watch: bool = False, watcher=None) -> Optional[Stat]:
        """Return the stat of the node of the given path

        Return null if no such a node exists.

        If the watcher is non-null and the call is successful (no error is raised),
        a watcher will be left on the node with the given path. The watcher will be
        triggered by a successful operation that creates/delete the node or sets
        the data on the node.

        Args:
            path: the node path
            watch: designate the default watcher associated with this connection
                to be the watcher
            watcher: explicit watcher

        Returns:
            The stat of the node of the given path; return null if no such a
            node exists.

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """

        LOGGER.debug("exists(%r, %r, %r)", path, watch, watcher)

        if watch and watcher:
            LOGGER.warning("Both watch and watcher were specified, registering watcher")

        request = ExistsRequest(_prefix_root(self.chroot, path), watch or watcher is not None)
        response = ExistsResponse(None)

        def register_watcher(exception):
            if not exception:
                with self._state_lock:
                    self._data_watchers[_prefix_root(self.chroot, path)].add(watcher or self._default_watcher)
            elif exception == NoNodeError:
                with self._state_lock:
                    self._exists_watchers[_prefix_root(self.chroot, path)].add(watcher or self._default_watcher)

        try:
            self._call(request, response, register_watcher if (watch or watcher) else lambda e: True)

            return response.stat if response.stat.czxid != -1 else None
        except NoNodeError:
            register_watcher(NoNodeError)
            return None

    @log_wrapper()
    def get_data(self, path: str, watch: bool = False, watcher=None) -> Tuple[bytearray, Stat]:
        """Return the data and the stat of the node of the given path

        If the watch is non-null and the call is successful (no error is
        raised), a watch will be left on the node with the given path. The watch
        will be triggered by a successful operation that sets data on the node, or
        deletes the node.

        NoNodeError will be raised if no node with the given path exists.

        Args:
            path: the given path
            watch: designate the default watcher associated with this connection
                to be the watcher
            watcher: explicit watcher

        Returns:
            The data of the node

        Raises:
            ZookeeperError: if the server returns a non-zero error code


        """

        LOGGER.debug("get_data(%r, %r, %r)", path, watch, watcher)

        if watch and watcher:
            LOGGER.warning("Both watch and watcher were specified, registering watcher")

        request = GetDataRequest(_prefix_root(self.chroot, path), watch or watcher is not None)
        response = GetDataResponse(None, None)

        def register_watcher(exception):
            if not exception:
                with self._state_lock:
                    self._data_watchers[_prefix_root(self.chroot, path)].add(watcher or self._default_watcher)

        self._call(request, response, register_watcher if (watch or watcher) else lambda e: True)

        return response.data, response.stat

    @log_wrapper()
    def set_data(self, path: str, data: bytearray, version: int = -1) -> Stat:
        """Set the data for the node of the given path

        Set the data for the node of the given path if such a node exists and the
        given version matches the version of the node (if the given version is
        -1, the default, it matches any node's versions). Return the stat of the node.

        This operation, if successful, will trigger all the watches on the node
        of the given path left by get_data() calls.

        NoNodeError will be raised if no node with the given path exists.

        BadVersionError will be raised if the given version does not match the node's version.

        The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
        Arrays larger than this will cause a ZookeeperError to be thrown.

        Args:
            path: the path of the node
            data: the data to set
            version: the expected matching version

        Returns:
            The state of the node

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """

        LOGGER.debug("set_data(%r, %r, %r)", path, data, version)

        request = SetDataRequest(_prefix_root(self.chroot, path), data, version)
        response = SetDataResponse(None)

        self._call(request, response)

        return response.stat

    @log_wrapper()
    def get_acls(self, path: str) -> Tuple[List[ACL], Stat]:
        """Return the ACL and stat of the node of the given path

        NoNodeError will be raised if no node with the given path exists.

        Args:
            path: the given path for the node

        Returns:
            The ACL array of the given node

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """

        LOGGER.debug("get_acls(%r)", path)

        request = GetACLRequest(_prefix_root(self.chroot, path))
        response = GetACLResponse(None, None)

        self._call(request, response)

        return response.acl, response.stat

    @log_wrapper()
    def set_acls(self, path: str, acls: List[ACL], version: int = -1) -> Stat:
        """Set the ACL for the node of the given path

        Set the ACL for the node of the given path if such a node exists and the
        given version matches the version of the node. Return the stat of the
        node.

        NoNodeError will be raised if no node with the given path exists.

        BadVersionError will be raised if the given version does not match the node's version.

        Args:
            path: the given path for the node
            acls: the ACLs to set
            version: the expected matching version

        Returns:
            The stat of the node.

        Raises:
            ZookeeperError: if the server returns a non-zero error code
            InvalidACLError: if the acl is invalid

        """

        LOGGER.debug("set_acls(%r, %r, %r)", path, acls, version)

        request = SetACLRequest(_prefix_root(self.chroot, path), acls, version)
        response = SetACLResponse(None)

        self._call(request, response)

        return response.stat

    @log_wrapper()
    def sync(self, path):
        """Asynchronous sync

        Flushes channel between process and leader.

        Args:
            path: the given path for the node

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """

        LOGGER.debug("sync(%r)", path)

        request = SyncRequest(_prefix_root(self.chroot, path))
        response = SyncResponse(None)

        self._call(request, response)

    @log_wrapper()
    def get_children(self, path: str, watch: bool = False, watcher=None) -> Tuple[List[str], Stat]:
        """Return the list of the children of the node of the given path

        If the watch is non-null and the call is successful (no error is raised),
        a watch will be left on the node with the given path. The watch will be
        triggered by a successful operation that deletes the node of the given
        path or creates/delete a child under the node.

        The list of children returned is not sorted and no guarantee is provided
        as to its natural or lexical order.

        NoNodeError will be raised if no node with the given path exists.

        Args:
            path: the given path
            watch: designate the default watcher associated with this connection
                to be the watcher of the children
            watcher: explicit watcher

        Returns:
            An unordered array of children of the node with the given path

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """

        LOGGER.debug("get_children(%r, %r, %r)", path, watch, watcher)

        if watch and watcher:
            LOGGER.warning("Both watch and watcher were specified, registering watcher")

        request = GetChildren2Request(_prefix_root(self.chroot, path), watch or watcher is not None)
        response = GetChildren2Response(None, None)

        def register_watcher(exception):
            if not exception:
                with self._state_lock:
                    self._child_watchers[_prefix_root(self.chroot, path)].add(watcher or self._default_watcher)

        self._call(request, response, register_watcher if (watch or watcher) else lambda e: True)

        return response.children, response.stat

    def _call(self, request, response, register_watcher=None):
        call_exception: Optional[BaseException] = None
        event = threading.Event()

        with self._state_lock:
            self._check_state()

            def callback(exception):
                nonlocal call_exception
                if exception:
                    call_exception = exception
                if register_watcher:
                    register_watcher(exception)

                event.set()

            self._queue.put((request, response, callback))

        event.wait()
        if call_exception:
            raise call_exception

    def _allocate_socket(self):
        """Used to allow the replacement of a socket with a mock socket"""
        return socket.socket()

    def _check_state(self):
        with self._state_lock:
            if self.state == AUTH_FAILED:
                raise AuthFailedError()
            if self.state == CLOSED:
                raise SessionExpiredError()
            if self.state == CONNECTION_DROPPED_FOR_TEST:
                raise ConnectionDroppedForTest()

    def _connected(self, session_id, session_passwd: bytearray, read_only):
        with self._state_lock:
            LOGGER.debug("Connected %s", "read-only mode" if read_only else "")

            self.state = CONNECTED_RO if read_only else CONNECTED
            self._events.put(lambda: self._default_watcher.session_connected(session_id, session_passwd, read_only))

    def _disconnected(self):
        assert self.state in {CONNECTING, CONNECTED, CONNECTED_RO, CONNECTION_DROPPED_FOR_TEST}
        with self._state_lock:
            if self.state in {CONNECTING, CONNECTION_DROPPED_FOR_TEST}:
                return

            LOGGER.debug("Disconnected %s %s pending calls", self.state, self._pending.qsize())
            LOGGER.debug("        %s %s queued calls", " " * len(str(self.state)), self._queue.qsize())

            self.state = CONNECTING

            self._events.put(lambda: self._default_watcher.connection_dropped())

            # drain queues
            self._drain(ConnectionLoss())

    def _closed(self, state, session_expired=False):
        """The party is over.  Time to clean up"""
        assert state in set([CLOSED, AUTH_FAILED, CONNECTION_DROPPED_FOR_TEST])
        with self._state_lock:
            self.state = state

            LOGGER.debug("CLOSING %s %s pending calls", state, self._pending.qsize())
            LOGGER.debug("        %s %s queued calls", " " * len(str(state)), self._queue.qsize())
            if session_expired:
                LOGGER.debug("        session expired")

            # notify watchers
            if state == AUTH_FAILED:
                self._events.put(lambda: self._default_watcher.auth_failed())
            elif session_expired:
                self._events.put(lambda: self._default_watcher.session_expired(self.session_id))
            else:
                self._events.put(lambda: self._default_watcher.connection_closed())

            # drain queues
            if state == CLOSED:
                self._drain(SessionExpiredError() if session_expired else ConnectionLoss())
            elif state == AUTH_FAILED:
                self._drain(AuthFailedError())

            # when the event thread encounters the connection on the queue, it
            # will kill itself
            self._events.stop()

    def _drain(self, error):
        assert self._state_lock._is_owned()

        while not self._pending.empty():
            _, _, callback, _ = self._pending.get()
            try:
                callback(error)
            except Exception:
                LOGGER.exception("Error while draining")

        while not self._queue.empty():
            _, _, callback = self._queue.get()
            try:
                callback(error)
            except Exception:
                LOGGER.exception("Error while draining")


class Client34(Client33):
    @log_wrapper()
    def __init__(
            self,
            hosts,
            session_id=None,
            session_passwd: bytearray = None,
            session_timeout=30.0,
            auth_data=None,
            read_only=False,
            watcher=None,
            allow_reconnect=True,
    ):
        Client33.__init__(self, hosts, session_id, session_passwd, session_timeout, auth_data, watcher, allow_reconnect)
        self.read_only = read_only

    @log_wrapper()
    def allocate_transaction(self):
        """Allocate a transaction

        A Transaction provides a builder object that can be used to construct
        and commit an atomic set of operations.

        Returns:
            A Transaction builder object

        """
        return _Transaction(self)

    def _multi(self, operations):
        request = TransactionRequest(operations)
        response = TransactionResponse(None)

        self._call(request, response)

        return response.results


class _Transaction:
    def __init__(self, client):
        self.client = client
        self.operations = []
        self.post_processors = []
        self.committed = False
        self.lock = threading.RLock()

    @log_wrapper()
    def create(self, path, acls, code, data=None):
        self._add(
            CreateRequest(_prefix_root(self.client.chroot, path), data, acls, code.flags),
            lambda x: x[len(self.client.chroot):],
        )

    @log_wrapper()
    def delete(self, path, version):
        self._add(DeleteRequest(_prefix_root(self.client.chroot, path), version))

    @log_wrapper()
    def set_data(self, path, data, version):
        self._add(SetDataRequest(_prefix_root(self.client.chroot, path), data, version))

    @log_wrapper()
    def check(self, path, version):
        self._add(CheckVersionRequest(_prefix_root(self.client.chroot, path), version))

    @log_wrapper()
    def commit(self):
        with self.lock:
            self._check_tx_state()
            self.committed = True
            LOGGER.debug("Committing on %r", self)

            results = []
            for e, p in zip(self.client._multi(self.operations), self.post_processors):
                if isinstance(e, str) or isinstance(e, str):
                    e = p(e)
                results.append(e)

            return results

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        """commit and cleanup accumulated transaction data structures"""
        if not type:
            self.commit()

    def _check_tx_state(self):
        if self.committed:
            raise ValueError("Transaction already committed")

    def _add(self, request, post_processor=None):
        with self.lock:
            self._check_tx_state()
            LOGGER.debug("Added %r to %r", request, self)
            self.operations.append(request)
            self.post_processors.append(post_processor if post_processor else lambda x: x)


def _prefix_root(root: str, path: str) -> str:
    """Prepend a root to a path."""
    return zkpath.normpath(zkpath.join(_norm_root(root), path.lstrip("/")))


def _norm_root(root: str) -> str:
    return zkpath.normpath(zkpath.join("/", root))
