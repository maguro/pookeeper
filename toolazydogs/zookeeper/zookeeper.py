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
from Queue import Queue
from collections import defaultdict
import logging
import socket
import threading

from toolazydogs.zookeeper import zkpath, SessionExpiredError, AuthFailedError, ConnectionLoss, Watcher, InvalidACLError
from toolazydogs.zookeeper import  NoNodeError, CONNECTING, CLOSED, AUTH_FAILED
from toolazydogs.zookeeper.hosts import collect_hosts
from toolazydogs.zookeeper.impl import WriterThread, PeekableQueue
from toolazydogs.zookeeper.packets.proto.CheckVersionRequest import CheckVersionRequest
from toolazydogs.zookeeper.packets.proto.CloseRequest import CloseRequest
from toolazydogs.zookeeper.packets.proto.CloseResponse import CloseResponse
from toolazydogs.zookeeper.packets.proto.CreateRequest import CreateRequest
from toolazydogs.zookeeper.packets.proto.CreateResponse import CreateResponse
from toolazydogs.zookeeper.packets.proto.DeleteRequest import DeleteRequest
from toolazydogs.zookeeper.packets.proto.ExistsRequest import ExistsRequest
from toolazydogs.zookeeper.packets.proto.ExistsResponse import ExistsResponse
from toolazydogs.zookeeper.packets.proto.GetACLRequest import GetACLRequest
from toolazydogs.zookeeper.packets.proto.GetACLResponse import GetACLResponse
from toolazydogs.zookeeper.packets.proto.GetChildren2Request import GetChildren2Request
from toolazydogs.zookeeper.packets.proto.GetChildren2Response import GetChildren2Response
from toolazydogs.zookeeper.packets.proto.GetDataRequest import GetDataRequest
from toolazydogs.zookeeper.packets.proto.GetDataResponse import GetDataResponse
from toolazydogs.zookeeper.packets.proto.SetACLRequest import SetACLRequest
from toolazydogs.zookeeper.packets.proto.SetACLResponse import SetACLResponse
from toolazydogs.zookeeper.packets.proto.SetDataRequest import SetDataRequest
from toolazydogs.zookeeper.packets.proto.SetDataResponse import SetDataResponse
from toolazydogs.zookeeper.packets.proto.SyncRequest import SyncRequest
from toolazydogs.zookeeper.packets.proto.SyncResponse import SyncResponse
from toolazydogs.zookeeper.packets.proto.TransactionRequest import TransactionRequest
from toolazydogs.zookeeper.packets.proto.TransactionResponse import TransactionResponse


LOGGER = logging.getLogger('toolazydogs.zookeeper')

class Client33(object):
    def __init__(self, hosts, session_id=None, session_passwd=None, session_timeout=30.0, auth_data=None, watcher=None):
        self.hosts, chroot = collect_hosts(hosts)
        if chroot:
            self.chroot = zkpath.normpath(chroot)
            if not zkpath.isabs(self.chroot):
                raise ValueError('chroot not absolute')
        else:
            self.chroot = ''

        self.session_id = session_id
        self.session_passwd = session_passwd if session_passwd else str(bytearray([0] * 16))
        self.session_timeout = session_timeout
        self.auth_data = auth_data if auth_data else set([])
        self.read_only = False

        self.last_zxid = 0

        self._queue = PeekableQueue()
        self._pending = Queue()

        self._events = Queue()
        self._child_watchers = defaultdict(set)
        self._data_watchers = defaultdict(set)
        self._exists_watchers = defaultdict(set)
        self._default_watcher = watcher or Watcher()

        self._state = CONNECTING
        self._state_lock = threading.RLock()

        self._event_thread_completed = threading.Event()

        def event_worker():
            try:
                while True:
                    notification = self._events.get()

                    if notification == self: break

                    try:
                        notification()
                    except Exception as e:
                        LOGGER.exception(e)
            finally:
                LOGGER.debug('Event loop completed')
                self._event_thread_completed.set()

        self._event_thread = threading.Thread(target=event_worker)
        self._event_thread.daemon = True
        self._event_thread.start()

        writer_thread = WriterThread(self)
        writer_thread.setDaemon(True)
        writer_thread.start()

        self._check_state()

    def close(self):
        """ Close this client object

        Once the client is closed, its session becomes invalid. All the
        ephemeral nodes in the ZooKeeper server associated with the session
        will be removed. The watches left on those nodes (and on their parents)
        will be triggered.
        """
        with self._state_lock:
            if self._state == AUTH_FAILED:
                return
            if self._state == CLOSED:
                return

            call_exception = None
            event = threading.Event()

            def close(exception):
                global call_exception
                call_exception = exception
                event.set()
                LOGGER.debug('Closing handler called')

            self._queue.put((CloseRequest(), CloseResponse(), close))

        event.wait()

        self._event_thread_completed.wait()

        self.session_id = None
        self.session_passwd = str(bytearray([0] * 16))

        if call_exception:
            raise call_exception

    def create(self, path, acls, code, data=None):
        """ Create a node with the given path

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
            acl: the acl for the node
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
        if not acls:
            raise InvalidACLError('ACLs cannot be None or empty')
        if not code:
            raise ValueError('Creation code cannot be None')
        request = CreateRequest(_prefix_root(self.chroot, path), data, acls, code.flags)
        response = CreateResponse(None)

        self._call(request, response)

        return response.path[len(self.chroot):]

    def delete(self, path, version=-1):
        """ Delete the node with the given path

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
        request = DeleteRequest(_prefix_root(self.chroot, path), version)

        self._call(request, None)

    def exists(self, path, watch=False, watcher=None):
        """ Return the stat of the node of the given path

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
        if watch and watcher:
            LOGGER.warn('Both watch and watcher were specified, registering watcher')

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
            self._call(request,
                       response,
                       lambda e: register_watcher(e) if (watch or watcher) else lambda: True)

            return response.stat if response.stat.czxid != -1 else None
        except NoNodeError:
            register_watcher(NoNodeError)
            return None

    def get_data(self, path, watch=False, watcher=None):
        """ Return the data and the stat of the node of the given path

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
        if watch and watcher:
            LOGGER.warn('Both watch and watcher were specified, registering watcher')

        request = GetDataRequest(_prefix_root(self.chroot, path), watch or watcher is not None)
        response = GetDataResponse(None, None)

        def register_watcher(exception):
            if not exception:
                with self._state_lock:
                    self._data_watchers[_prefix_root(self.chroot, path)].add(watcher or self._default_watcher)

        self._call(request,
                   response,
                   lambda e: register_watcher(e) if (watch or watcher) else lambda: True)

        return response.data, response.stat

    def set_data(self, path, data, version=-1):
        """ Set the data for the node of the given path

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
        request = SetDataRequest(_prefix_root(self.chroot, path), data, version)
        response = SetDataResponse(None)

        self._call(request, response)

        return response.stat

    def get_acls(self, path):
        """ Return the ACL and stat of the node of the given path

        NoNodeError will be raised if no node with the given path exists.

        Args:
            path: the given path for the node

        Returns:
            The ACL array of the given node

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """
        request = GetACLRequest(_prefix_root(self.chroot, path))
        response = GetACLResponse(None, None)

        self._call(request, response)

        return response.acl, response.stat

    def set_acls(self, path, acls, version=-1):
        """ Set the ACL for the node of the given path

        Set the ACL for the node of the given path if such a node exists and the
        given version matches the version of the node. Return the stat of the
        node.

        NoNodeError will be raised if no node with the given path exists.

        BadVersionError will be raised if the given version does not match the node's version.

        Args:
            path: the given path for the node
            acl: the ACLs to set
            version: the expected matching version

        Returns:
            The stat of the node.

        Raises:
            ZookeeperError: if the server returns a non-zero error code
            InvalidACLError: if the acl is invalid

        """
        request = SetACLRequest(_prefix_root(self.chroot, path), acls, version)
        response = SetACLResponse(None)

        self._call(request, response)

        return response.stat

    def sync(self, path):
        """ Asynchronous sync

        Flushes channel between process and leader.

        Args:
            path: the given path for the node

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """
        request = SyncRequest(_prefix_root(self.chroot, path))
        response = SyncResponse(None)

        self._call(request, response)

    def get_children(self, path, watch=False, watcher=None):
        """ Return the list of the children of the node of the given path

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
                to be the watcher
            watcher: explicit watcher

        Returns:
            An unordered array of children of the node with the given path

        Raises:
            ZookeeperError: if the server returns a non-zero error code

        """
        if watch and watcher:
            LOGGER.warn('Both watch and watcher were specified, registering watcher')

        request = GetChildren2Request(_prefix_root(self.chroot, path), watch or watcher is not None)
        response = GetChildren2Response(None, None)

        def register_watcher(exception):
            if not exception:
                with self._state_lock:
                    self._child_watchers[_prefix_root(self.chroot, path)].add(watcher or self._default_watcher)

        self._call(request,
                   response,
                   lambda e: register_watcher(e) if (watch or watcher) else lambda: True)

        return response.children, response.stat

    def _call(self, request, response, register_watcher=None):
        with self._state_lock:
            self._check_state()

            call_exception = [None]
            event = threading.Event()

            def callback(exception):
                if exception:
                    call_exception[0] = exception
                if register_watcher:
                    register_watcher(exception)

                event.set()

            self._queue.put((request, response, callback))

        event.wait()
        if call_exception[0]:
            raise call_exception[0]

    def _allocate_socket(self):
        """ Used to allow the replacement of a socket with a mock socket
        """
        return socket.socket()


    def _check_state(self):
        if self._state == AUTH_FAILED:
            raise AuthFailedError()
        if self._state == CLOSED:
            raise SessionExpiredError()

    def _close(self, state):
        """ The party is over.  Time to clean up
        """
        assert state in set([CLOSED, AUTH_FAILED])
        with self._state_lock:
            self._state = state

            # notify watchers
            self._events.put(lambda: self._default_watcher.connection_closed())

            LOGGER.debug('CLOSING %s %s pending calls', state, self._pending.qsize())
            LOGGER.debug('        %s %s queued calls', ' ' * len(str(state)), self._queue.qsize())

            # drain the pending queue
            while not self._pending.empty():
                _, _, callback, _ = self._pending.get()
                if state == CLOSED:
                    callback(ConnectionLoss())
                elif state == AUTH_FAILED:
                    callback(AuthFailedError())

            while not self._queue.empty():
                _, _, callback = self._queue.get()
                if state == CLOSED:
                    callback(ConnectionLoss())
                elif state == AUTH_FAILED:
                    callback(AuthFailedError())

            # when the event thread encounters the connection on the queue, it
            # will kill itself
            self._events.put(self)


class Client34(Client33):
    def __init__(self, hosts, session_id=None, session_passwd=None, session_timeout=30.0, auth_data=None, read_only=False, watcher=None):
        Client33.__init__(self, hosts, session_id, session_passwd, session_timeout, auth_data, watcher)
        self.read_only = read_only

    def allocate_transaction(self):
        """ Allocate a transaction

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


class _Transaction(object):
    def __init__(self, client):
        self.client = client
        self.operations = []
        self.post_processors = []
        self.committed = False
        self.lock = threading.RLock()

    def create(self, path, acls, code, data=None):
        self._add(CreateRequest(_prefix_root(self.client.chroot, path), data, acls, code.flags),
                  lambda x: x[len(self.client.chroot):])

    def delete(self, path, version):
        self._add(DeleteRequest(_prefix_root(self.client.chroot, path), version))

    def set_data(self, path, data, version):
        self._add(SetDataRequest(_prefix_root(self.client.chroot, path), data, version))

    def check(self, path, version):
        self._add(CheckVersionRequest(_prefix_root(self.client.chroot, path), version))

    def commit(self):
        with self.lock:
            self._check_tx_state()
            self.committed = True
            LOGGER.debug('Committing on %r', self)

            results = []
            for e, p in zip(self.client._multi(self.operations), self.post_processors):
                if isinstance(e, str) or isinstance(e, unicode):
                    e = p(e)
                results.append(e)

            return results

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        """ commit and cleanup accumulated transaction data structures
        """
        if not type:
            self.commit()

    def _check_tx_state(self):
        if self.committed:
            raise ValueError('Transaction already committed')

    def _add(self, request, post_processor=None):
        with self.lock:
            self._check_tx_state()
            LOGGER.debug('Added %r to %r', request, self)
            self.operations.append(request)
            self.post_processors.append(post_processor if post_processor else lambda x: x)


def _prefix_root(root, path):
    """ Prepend a root to a path. """
    return zkpath.normpath(zkpath.join(_norm_root(root), path.lstrip('/')))


def _norm_root(root):
    return zkpath.normpath(zkpath.join('/', root))
