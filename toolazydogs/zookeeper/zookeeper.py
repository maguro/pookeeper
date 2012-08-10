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

from toolazydogs.zookeeper import zkpath, SessionExpiredError, AuthFailedError, ConnectionLoss, Watcher
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
    def __init__(self, hosts, session_id=None, session_passwd=None, session_timeout=30.0, auth_data=None, read_only=False, watcher=None):
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
        self.read_only = read_only

        self.last_zxid = 0

        self._queue = PeekableQueue()
        self._pending = Queue()

        self._events = Queue()
        self.child_watchers = defaultdict(set)
        self.data_watchers = defaultdict(set)
        self.exists_watchers = defaultdict(set)
        self.default_watcher = watcher or Watcher()

        self._state = CONNECTING
        self._state_lock = threading.RLock()

        def event_worker():
            while True:
                notification = self._events.get()

                if notification == self: break

                try:
                    notification()
                except Exception as e:
                    LOGGER.exception(e)

        self._event_thread = threading.Thread(target=event_worker)
        self._event_thread.daemon = True
        self._event_thread.start()

        self.writer_started = threading.Event()

        writer_thread = WriterThread(self, self.writer_started)
        writer_thread.setDaemon(True)
        writer_thread.start()

        self.writer_started.wait()

        self._check_state()

    def close(self):
        with self._state_lock:
            self._check_state()

            call_exception = None
            event = threading.Event()

            def close(exception):
                global call_exception
                call_exception = exception
                event.set()
                LOGGER.debug('Closing handler called')

            self._queue.put((CloseRequest(), CloseResponse(), close))

        event.wait()

        self.session_id = None
        self.session_passwd = str(bytearray([0] * 16))

        if call_exception:
            raise call_exception

    def create(self, path, acls, code, data=None):
        if not acls:
            raise ValueError('ACLs cannot be None or empty')
        if not code:
            raise ValueError('Creation code cannot be None')
        request = CreateRequest(_prefix_root(self.chroot, path), data, acls, code.flags)
        response = CreateResponse(None)

        self._call(request, response)

        return response.path[len(self.chroot):]

    def delete(self, path, version):
        request = DeleteRequest(_prefix_root(self.chroot, path), version)

        self._call(request, None)

    def exists(self, path, watch=False, watcher=None):
        request = ExistsRequest(_prefix_root(self.chroot, path), watch)
        response = ExistsResponse(None)

        try:
            def register_watcher(exception):
                if not exception:
                    with self._state_lock:
                        self.data_watchers[_prefix_root(self.chroot, path)].add(watcher or self.default_watcher)
                elif exception == NoNodeError:
                    with self._state_lock:
                        self.exists_watchers[_prefix_root(self.chroot, path)].add(watcher or self.default_watcher)

            self._call(request,
                       response,
                       lambda e: register_watcher(e) if (watch or watcher) else lambda: True)

            return response.stat if response.stat.czxid != -1 else None
        except NoNodeError:
            return None

    def get_data(self, path, watch=False, watcher=None):
        request = GetDataRequest(_prefix_root(self.chroot, path), watch)
        response = GetDataResponse(None, None)

        def register_watcher(exception):
            if not exception:
                self.data_watchers[_prefix_root(self.chroot, path)].add(watcher or self.default_watcher)

        self._call(request,
                   response,
                   lambda e: register_watcher(e) if (watch or watcher) else lambda: True)

        return response.data, response.stat

    def set_data(self, path, data, version):
        request = SetDataRequest(_prefix_root(self.chroot, path), data, version)
        response = SetDataResponse(None)

        self._call(request, response)

        return response.stat

    def get_acls(self, path):
        request = GetACLRequest(_prefix_root(self.chroot, path))
        response = GetACLResponse(None, None)

        self._call(request, response)

        return response.acl, response.stat

    def set_acls(self, path, acls, version):
        request = SetACLRequest(_prefix_root(self.chroot, path), acls, version)
        response = SetACLResponse(None)

        self._call(request, response)

        return response.stat

    def sync(self, path):
        request = SyncRequest(_prefix_root(self.chroot, path))
        response = SyncResponse(None)

        self._call(request, response)

    def get_children(self, path, watch=False, watcher=None):
        request = GetChildren2Request(_prefix_root(self.chroot, path), watch)
        response = GetChildren2Response(None, None)

        def register_watcher(exception):
            if not exception:
                with self._state_lock:
                    self.child_watchers[_prefix_root(self.chroot, path)].add(watcher or self.default_watcher)

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

    def _all_watchers(self):
        with self._state_lock:
            watchers = set()
            watchers.add(self.default_watcher)
            for v in self.child_watchers.itervalues():
                watchers.add(v)
            for v in self.data_watchers.itervalues():
                watchers.add(v)
            for v in self.exists_watchers.itervalues():
                watchers.add(v)
            return watchers

    def _close(self, state):
        """ The party is over.  Time to clean up
        """
        assert state in set([CLOSED, AUTH_FAILED])
        with self._state_lock:
            self._state = state

            # People may be waiting for the writer to start even though it
            # has already aborted the mission.
            self.writer_started.set()

            # notify watchers
            self._events.put(lambda: map(lambda w: w.connection_closed(), self._all_watchers()))

            # drain the pending queue
            while not self._pending.empty():
                request, response, callback, xid = self._pending.get()
                if state == CLOSED:
                    callback(ConnectionLoss())
                elif state == AUTH_FAILED:
                    callback(AuthFailedError())

            # when the event thread encounters the connection on the queue, it
            # will kill itself
            self._events.put(self)


class Client34(Client33):
    def __init__(self, hosts, session_id=None, session_passwd=None, session_timeout=30.0, auth_data=None, read_only=False, watcher=None):
        Client33.__init__(self, hosts, session_id, session_passwd, session_timeout, auth_data, read_only, watcher)

    def allocate_transaction(self):
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
