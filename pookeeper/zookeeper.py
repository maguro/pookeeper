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

from __future__ import annotations

import logging
import socket
import threading
from collections import defaultdict
from collections.abc import Callable
from queue import Queue
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar

from pookeeper import (
    AUTH_FAILED,
    CLOSED,
    CONNECTED,
    CONNECTED_RO,
    CONNECTING,
    CONNECTION_DROPPED_FOR_TEST,
    OPEN_ACL_UNSAFE,
    AuthFailedError,
    ConnectionLoss,
    CreateCode,
    InvalidACLError,
    NoNodeError,
    Persistent,
    SessionExpiredError,
    State,
    Watcher,
    WatchersDict,
    events,
    zkpath,
)
from pookeeper.hosts import collect_hosts
from pookeeper.impl import ConnectionDroppedForTest, PeekableQueue, WriterThread
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
from pookeeper.session import Session

if TYPE_CHECKING:
    from types import TracebackType

    from pookeeper import ZookeeperError
    from pookeeper._typing import (
        AuthData,
        Deserializable,
        PendingCall,
        Request,
        Self,
        TransactionResult,
        WatcherRegistration,
    )
    from pookeeper.packets.data.ACL import ACL
    from pookeeper.packets.data.Stat import Stat

_logger = logging.getLogger(__name__)

_ID = 0
_ID_LOCK = threading.RLock()

_P = ParamSpec("_P")
_R = TypeVar("_R")
_Method = Callable[Concatenate[Any, _P], _R]


def log_wrapper() -> Callable[[_Method[_P, _R]], _Method[_P, _R]]:
    """A class method decorator that renames the current thread.

    The new name identifies the current pookeeper client.
    """

    def wrapper(method: _Method[_P, _R]) -> _Method[_P, _R]:
        def new(self: Any, *args: _P.args, **kws: _P.kwargs) -> _R:
            global _ID
            try:
                name = f"pookeeper-{self.id}"
            except AttributeError:
                with _ID_LOCK:
                    self.id = _ID
                    _ID += 1
                name = f"pookeeper-{self.id}"
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
    """A client for the ZooKeeper 3.3 operations.

    Create one with pookeeper.allocate_33(), which takes the same arguments as
    this constructor. Use it as a context manager, or call close() when done.

    Attributes:
        state: the connection State, for example CONNECTED
        session: the Session. Its id and passwd resume the session in a new
            client.
        chroot: the path that all operations are relative to, or "" for none
        default_acl: the ACL that create() uses when it is given no ACL
        negotiated_session_timeout: the session timeout in seconds that the
            server agreed to. It is set when the client first connects.
    """

    id: int
    negotiated_session_timeout: float

    @log_wrapper()
    def __init__(
        self,
        hosts: str,
        session_id: int | None = None,
        session_passwd: bytearray | None = None,
        session_timeout: float = 30.0,
        auth_data: AuthData | None = None,
        watcher: Watcher | None = None,
        allow_reconnect: bool = True,
        default_acl: list[ACL] | None = None,
    ) -> None:
        self.hosts, chroot = collect_hosts(hosts)
        if chroot:
            self.chroot = zkpath.normpath(chroot)
            if not zkpath.isabs(self.chroot):
                raise ValueError("chroot not absolute")
        else:
            self.chroot = ""

        self.session = Session(session_id=session_id, session_passwd=session_passwd)
        self.session_timeout = session_timeout
        self.connect_timeout = session_timeout / len(self.hosts)
        self.read_timeout = session_timeout * 2.0 / 3.0
        self.auth_data: AuthData = auth_data if auth_data else set()
        self.read_only = False

        if _logger.isEnabledFor(logging.DEBUG):
            encoded_session_password = (
                "".join(f"{x:02x}" for x in session_passwd)
                if session_passwd
                else "None"
            )

            _logger.debug("session_id: %s", self.session.id)
            _logger.debug("session_passwd: 0x%s", encoded_session_password)
            _logger.debug("session_timeout: %s", self.session_timeout)
            _logger.debug("connect_timeout: %s", self.connect_timeout)
            _logger.debug("   len(hosts): %s", len(self.hosts))
            _logger.debug("read_timeout: %s", self.read_timeout)
            _logger.debug("auth_data: %s", self.auth_data)

        self.allow_reconnect = allow_reconnect
        _logger.debug("allow_reconnect: %s", self.allow_reconnect)

        self.default_acl = OPEN_ACL_UNSAFE if default_acl is None else default_acl
        _logger.debug("default_acl: %s", self.default_acl)

        self._queue = PeekableQueue()
        self._pending: Queue[PendingCall] = Queue()

        self._child_watchers: WatchersDict = defaultdict(set)
        self._data_watchers: WatchersDict = defaultdict(set)
        self._exists_watchers: WatchersDict = defaultdict(set)
        self._default_watcher: Watcher = watcher or Watcher()

        self.state: State = CONNECTING
        self._state_lock = threading.RLock()

        self._events = events.Events(self.id)
        self._events.start()

        self._writer_thread = WriterThread(self, self._events)
        self._writer_thread.daemon = True
        self._writer_thread.start()

        self._check_state()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    @log_wrapper()
    def close(self) -> None:
        """Close this client object

        Once the client is closed, its session becomes invalid. All the
        ephemeral nodes in the ZooKeeper server associated with the session
        will be removed. The watches left on those nodes (and on their parents)
        will be triggered.
        """

        _logger.debug("close()")

        call_exception: BaseException | None = None

        with self._state_lock:
            if self.state == AUTH_FAILED:
                return
            if self.state == CLOSED:
                return

            def close(exception: ZookeeperError | None) -> None:
                nonlocal call_exception
                call_exception = exception
                _logger.debug("Closing handler called")

            self._queue.put((CloseRequest(), CloseResponse(), close))

        # the events queue will be stopped when the writer thread closes
        self._events.join()

        self.session = Session()

        if call_exception:
            raise call_exception

    @log_wrapper()
    def create(
        self,
        path: str,
        acls: list[ACL] | None = None,
        code: CreateCode | None = None,
        data: bytearray | None = None,
    ) -> str:
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
            acls: the acl for the node. The default is the client's default_acl.
            code: specifying whether the node to be created is ephemeral
                and/or sequential. The default is Persistent.
            data: optional initial data for the node

        Returns:
            the actual path of the created node

        Raises:
            ZookeeperError: if the server returns a non-zero error code
            InvalidACLError: if the ACL is invalid or empty
            ValueError: if an invalid path is specified

        """

        _logger.debug("create(%r, %r, %r, %r)", path, acls, code, data)

        if acls is None:
            acls = self.default_acl
        if not acls:
            raise InvalidACLError("ACLs cannot be empty")
        if code is None:
            code = Persistent()

        request = CreateRequest(_prefix_root(self.chroot, path), data, acls, code.flags)
        response = CreateResponse(None)

        self._call(request, response)

        return response.path[len(self.chroot) :]

    @log_wrapper()
    def delete(self, path: str, version: int = -1) -> None:
        """Delete the node with the given path

        The call will succeed if such a node exists, and the given version
        matches the node's version (if the given version is -1, the default,
        it matches any node's versions).

        A NoNodeError will be raised if the node does not exist.

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

        _logger.debug("delete(%r, %r)", path, version)

        request = DeleteRequest(_prefix_root(self.chroot, path), version)

        self._call(request, None)

    @log_wrapper()
    def exists(
        self, path: str, watch: bool = False, watcher: Watcher | None = None
    ) -> Stat | None:
        """Return the stat of the node of the given path

        If watch is True or a watcher is given, a watch is left on the node,
        even if the node does not exist. The watch fires when the node is
        created or deleted, or when its data is set.

        Args:
            path: the node path
            watch: whether to set a watch that notifies the client's default
                watcher
            watcher: a watcher to notify instead of the default watcher

        Returns:
            The stat of the node, or None if the node does not exist.

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """

        _logger.debug("exists(%r, %r, %r)", path, watch, watcher)

        if watch and watcher:
            _logger.warning(
                "Both watch and watcher were specified, registering watcher"
            )

        request = ExistsRequest(
            _prefix_root(self.chroot, path), watch or watcher is not None
        )
        response = ExistsResponse(None)

        def register_watcher(exception: ZookeeperError | None) -> None:
            if not exception:
                with self._state_lock:
                    self._data_watchers[_prefix_root(self.chroot, path)].add(
                        watcher or self._default_watcher
                    )
            elif isinstance(exception, NoNodeError):
                with self._state_lock:
                    self._exists_watchers[_prefix_root(self.chroot, path)].add(
                        watcher or self._default_watcher
                    )

        try:
            self._call(
                request,
                response,
                register_watcher if (watch or watcher) else lambda e: True,
            )
        except NoNodeError:
            return None
        else:
            return response.stat if response.stat.czxid != -1 else None

    @log_wrapper()
    def get_data(
        self, path: str, watch: bool = False, watcher: Watcher | None = None
    ) -> tuple[bytearray, Stat]:
        """Return the data and the stat of the node of the given path

        If watch is True or a watcher is given and the call succeeds, a watch
        is left on the node. The watch fires when the data of the node is set
        or the node is deleted.

        NoNodeError will be raised if no node with the given path exists.

        Args:
            path: the node path
            watch: whether to set a watch that notifies the client's default
                watcher
            watcher: a watcher to notify instead of the default watcher

        Returns:
            A tuple of the data of the node and its stat.

        Raises:
            ZookeeperError: if the server returns a non-zero error code


        """

        _logger.debug("get_data(%r, %r, %r)", path, watch, watcher)

        if watch and watcher:
            _logger.warning(
                "Both watch and watcher were specified, registering watcher"
            )

        request = GetDataRequest(
            _prefix_root(self.chroot, path), watch or watcher is not None
        )
        response = GetDataResponse(None, None)

        def register_watcher(exception: ZookeeperError | None) -> None:
            if not exception:
                with self._state_lock:
                    self._data_watchers[_prefix_root(self.chroot, path)].add(
                        watcher or self._default_watcher
                    )

        self._call(
            request,
            response,
            register_watcher if (watch or watcher) else lambda e: True,
        )

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

        BadVersionError will be raised if the given version does not match the
        node's version.

        The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
        Arrays larger than this will cause a ZookeeperError to be thrown.

        Args:
            path: the path of the node
            data: the data to set
            version: the expected matching version

        Returns:
            The new stat of the node.

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """

        _logger.debug("set_data(%r, %r, %r)", path, data, version)

        request = SetDataRequest(_prefix_root(self.chroot, path), data, version)
        response = SetDataResponse(None)

        self._call(request, response)

        return response.stat

    @log_wrapper()
    def get_acls(self, path: str) -> tuple[list[ACL], Stat]:
        """Return the ACL and stat of the node of the given path

        NoNodeError will be raised if no node with the given path exists.

        Args:
            path: the given path for the node

        Returns:
            A tuple of the ACL list of the node and its stat.

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """

        _logger.debug("get_acls(%r)", path)

        request = GetACLRequest(_prefix_root(self.chroot, path))
        response = GetACLResponse(None, None)

        self._call(request, response)

        return response.acl, response.stat

    @log_wrapper()
    def set_acls(self, path: str, acls: list[ACL], version: int = -1) -> Stat:
        """Set the ACL for the node of the given path

        Set the ACL for the node of the given path if such a node exists and the
        given version matches the version of the node. Return the stat of the
        node.

        NoNodeError will be raised if no node with the given path exists.

        BadVersionError will be raised if the given version does not match the
        node's version.

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

        _logger.debug("set_acls(%r, %r, %r)", path, acls, version)

        request = SetACLRequest(_prefix_root(self.chroot, path), acls, version)
        response = SetACLResponse(None)

        self._call(request, response)

        return response.stat

    @log_wrapper()
    def sync(self, path: str) -> None:
        """Bring the connected server up to date with the leader

        Call it before a read that must see every write the leader has
        committed. It returns when the server has caught up.

        Args:
            path: the path of the node to sync

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """

        _logger.debug("sync(%r)", path)

        request = SyncRequest(_prefix_root(self.chroot, path))
        response = SyncResponse(None)

        self._call(request, response)

    @log_wrapper()
    def get_children(
        self, path: str, watch: bool = False, watcher: Watcher | None = None
    ) -> tuple[list[str], Stat]:
        """Return the names of the children of the node of the given path

        If watch is True or a watcher is given and the call succeeds, a watch
        is left on the node. The watch fires when the node is deleted, or when
        a child is added to or removed from it.

        NoNodeError will be raised if no node with the given path exists.

        Args:
            path: the node path
            watch: whether to set a watch that notifies the client's default
                watcher
            watcher: a watcher to notify instead of the default watcher

        Returns:
            A tuple of the names of the children, in no particular order, and
            the stat of the node.

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """

        _logger.debug("get_children(%r, %r, %r)", path, watch, watcher)

        if watch and watcher:
            _logger.warning(
                "Both watch and watcher were specified, registering watcher"
            )

        request = GetChildren2Request(
            _prefix_root(self.chroot, path), watch or watcher is not None
        )
        response = GetChildren2Response(None, None)

        def register_watcher(exception: ZookeeperError | None) -> None:
            if not exception:
                with self._state_lock:
                    self._child_watchers[_prefix_root(self.chroot, path)].add(
                        watcher or self._default_watcher
                    )

        self._call(
            request,
            response,
            register_watcher if (watch or watcher) else lambda e: True,
        )

        return response.children, response.stat

    def _call(
        self,
        request: Request,
        response: Deserializable | None,
        register_watcher: WatcherRegistration | None = None,
    ) -> None:
        call_exception: BaseException | None = None
        event = threading.Event()

        with self._state_lock:
            self._check_state()

            def callback(exception: ZookeeperError | None) -> None:
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

    def _allocate_socket(self) -> socket.socket:
        """Used to allow the replacement of a socket with a mock socket"""
        return socket.socket()

    def _check_state(self) -> None:
        with self._state_lock:
            if self.state == AUTH_FAILED:
                raise AuthFailedError()
            if self.state == CLOSED:
                raise SessionExpiredError()
            if self.state == CONNECTION_DROPPED_FOR_TEST:
                raise ConnectionDroppedForTest()

    def _connected(
        self, session_id: int, session_passwd: bytearray, read_only: bool
    ) -> None:
        with self._state_lock:
            _logger.debug("Connected %s", "read-only mode" if read_only else "")

            self.state = CONNECTED_RO if read_only else CONNECTED
            self._events.put(
                lambda: self._default_watcher.session_connected(
                    session_id, session_passwd, read_only
                )
            )

    def _disconnected(self) -> None:
        assert self.state in {  # noqa: S101
            CONNECTING,
            CONNECTED,
            CONNECTED_RO,
            CONNECTION_DROPPED_FOR_TEST,
        }
        with self._state_lock:
            if self.state in {CONNECTING, CONNECTION_DROPPED_FOR_TEST}:
                return

            _logger.debug(
                "Disconnected %s %s pending calls", self.state, self._pending.qsize()
            )
            _logger.debug(
                "        %s %s queued calls",
                " " * len(str(self.state)),
                self._queue.qsize(),
            )

            self.state = CONNECTING

            self._events.put(lambda: self._default_watcher.connection_dropped())

            # drain queues
            self._drain(ConnectionLoss())

    def _closed(self, state: State, session_expired: bool = False) -> None:
        """The party is over.  Time to clean up"""
        assert state in {CLOSED, AUTH_FAILED, CONNECTION_DROPPED_FOR_TEST}  # noqa: S101
        with self._state_lock:
            self.state = state

            _logger.debug("CLOSING %s %s pending calls", state, self._pending.qsize())
            _logger.debug(
                "        %s %s queued calls", " " * len(str(state)), self._queue.qsize()
            )
            if session_expired:
                _logger.debug("        session expired")

            # notify watchers
            if state == AUTH_FAILED:
                self._events.put(lambda: self._default_watcher.auth_failed())
            elif session_expired:
                self._events.put(
                    lambda: self._default_watcher.session_expired(self.session.id)
                )
            else:
                self._events.put(lambda: self._default_watcher.connection_closed())

            # drain queues
            if state == CLOSED:
                self._drain(
                    SessionExpiredError() if session_expired else ConnectionLoss()
                )
            elif state == AUTH_FAILED:
                self._drain(AuthFailedError())

            # when the event thread encounters the connection on the queue, it
            # will kill itself
            self._events.stop()

    def _drain(self, error: ZookeeperError) -> None:
        assert self._state_lock._is_owned()  # type: ignore[attr-defined]  # noqa: S101

        while not self._pending.empty():
            _, _, callback, _ = self._pending.get()
            try:
                callback(error)
            except Exception:
                _logger.exception("Error while draining")

        while not self._queue.empty():
            _, _, callback = self._queue.get()
            try:
                callback(error)
            except Exception:
                _logger.exception("Error while draining")


class Client34(Client33):
    """A client for the ZooKeeper 3.4 operations.

    It adds transactions and read-only mode to Client33. Create one with
    pookeeper.allocate(), which takes the same arguments as this constructor.
    """

    @log_wrapper()
    def __init__(
        self,
        hosts: str,
        session_id: int | None = None,
        session_passwd: bytearray | None = None,
        session_timeout: float = 30.0,
        auth_data: AuthData | None = None,
        read_only: bool = False,
        watcher: Watcher | None = None,
        allow_reconnect: bool = True,
        default_acl: list[ACL] | None = None,
    ) -> None:
        Client33.__init__(
            self,
            hosts,
            session_id,
            session_passwd,
            session_timeout,
            auth_data,
            watcher,
            allow_reconnect,
            default_acl,
        )
        self.read_only = read_only

    @log_wrapper()
    def allocate_transaction(self) -> _Transaction:
        """Allocate a transaction

        A Transaction provides a builder object that can be used to construct
        and commit an atomic set of operations.

        Returns:
            A Transaction builder object

        """
        return _Transaction(self)

    def _multi(self, operations: list[Request]) -> list[TransactionResult]:
        request = TransactionRequest(operations)
        response = TransactionResponse(None)

        self._call(request, response)

        return response.results


class _Transaction:
    """Operations that the server applies together, or not at all.

    Get one from Client34.allocate_transaction(). Add operations, then call
    commit(). Used as a context manager, it commits when the with block ends,
    unless the block raised an exception.
    """

    def __init__(self, client: Client34) -> None:
        self.client = client
        self.operations: list[Request] = []
        self.post_processors: list[Callable[[str], str]] = []
        self.committed = False
        self.lock = threading.RLock()

    @log_wrapper()
    def create(
        self,
        path: str,
        acls: list[ACL] | None = None,
        code: CreateCode | None = None,
        data: bytearray | None = None,
    ) -> None:
        """Add the creation of a node.

        The arguments are the same as those of Client33.create().

        Raises:
            ValueError: if the transaction was already committed
        """
        if acls is None:
            acls = self.client.default_acl
        if code is None:
            code = Persistent()
        self._add(
            CreateRequest(
                _prefix_root(self.client.chroot, path), data, acls, code.flags
            ),
            lambda x: x[len(self.client.chroot) :],
        )

    @log_wrapper()
    def delete(self, path: str, version: int) -> None:
        """Add the deletion of a node.

        Args:
            path: the path of the node to delete
            version: the version the node must have, or -1 for any version

        Raises:
            ValueError: if the transaction was already committed
        """
        self._add(DeleteRequest(_prefix_root(self.client.chroot, path), version))

    @log_wrapper()
    def set_data(self, path: str, data: bytearray, version: int) -> None:
        """Add setting the data of a node.

        Args:
            path: the path of the node
            data: the new data
            version: the version the node must have, or -1 for any version

        Raises:
            ValueError: if the transaction was already committed
        """
        self._add(SetDataRequest(_prefix_root(self.client.chroot, path), data, version))

    @log_wrapper()
    def check(self, path: str, version: int) -> None:
        """Add a check that a node has a version.

        The transaction fails if the check fails.

        Args:
            path: the path of the node
            version: the version the node must have

        Raises:
            ValueError: if the transaction was already committed
        """
        self._add(CheckVersionRequest(_prefix_root(self.client.chroot, path), version))

    @log_wrapper()
    def commit(self) -> list[TransactionResult]:
        """Send the operations to the server, which applies all or none of them.

        Returns:
            One result for each operation, in the order the operations were
            added. When the transaction succeeds, a create gives the path of
            the new node, set_data gives the new stat of the node, and delete
            and check give an empty tuple. When it fails, the server applies
            no operation and every result is a ZookeeperError: the operation
            that failed gives its error, the operations before it give
            RolledBackError, and the operations after it give
            RuntimeInconsistency.

        Raises:
            ValueError: if the transaction was already committed
        """
        with self.lock:
            self._check_tx_state()
            self.committed = True
            _logger.debug("Committing on %r", self)

            results: list[TransactionResult] = []
            for e, p in zip(
                self.client._multi(self.operations), self.post_processors, strict=False
            ):
                if isinstance(e, str):
                    e = p(e)
                results.append(e)

            return results

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Commit the transaction, unless the with block raised an exception."""
        if not exc_type:
            self.commit()

    def _check_tx_state(self) -> None:
        if self.committed:
            raise ValueError("Transaction already committed")

    def _add(
        self, request: Request, post_processor: Callable[[str], str] | None = None
    ) -> None:
        with self.lock:
            self._check_tx_state()
            _logger.debug("Added %r to %r", request, self)
            self.operations.append(request)
            self.post_processors.append(
                post_processor if post_processor else lambda x: x
            )


def _prefix_root(root: str, path: str) -> str:
    """Prepend a root to a path."""
    return zkpath.normpath(zkpath.join(_norm_root(root), path.lstrip("/")))


def _norm_root(root: str) -> str:
    return zkpath.normpath(zkpath.join("/", root))
