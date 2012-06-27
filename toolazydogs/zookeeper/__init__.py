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
from Queue import Queue, Empty, Full
from collections import defaultdict

from time import time as _time
from toolazydogs.zookeeper import packets
from toolazydogs.zookeeper.packets.data.ACL import ACL
from toolazydogs.zookeeper.packets.data.Id import Id


__version__ = '1.0.0-dev'

class PeekableQueue(Queue):
    def __init__(self, maxsize=0):
        Queue.__init__(self, maxsize=0)

    def peek(self, block=True, timeout=None):
        """Return the first item in the queue but do not remove it from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a positive number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        """
        self.not_empty.acquire()
        try:
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a positive number")
            else:
                endtime = _time() + timeout
                while not self._qsize():
                    remaining = endtime - _time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            item = self.queue[0]
            return item
        finally:
            self.not_empty.release()

    def put_front(self, item, block=True, timeout=None):
        """Put an item into the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a positive number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).
        """
        self.not_full.acquire()
        try:
            if self.maxsize > 0:
                if not block:
                    if self._qsize() == self.maxsize:
                        raise Full
                elif timeout is None:
                    while self._qsize() == self.maxsize:
                        self.not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a positive number")
                else:
                    endtime = _time() + timeout
                    while self._qsize() == self.maxsize:
                        remaining = endtime - _time()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.wait(remaining)
            self.queue.appendLeft(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()
        finally:
            self.not_full.release()


class Watcher(object):
    def sessionConnected(self, session_id, session_password, read_only):
        pass

    def sessionExpired(self, session_id):
        pass

    def connectionDropped(self):
        pass

    def connectionClosed(self):
        pass

    def node_created(self, path):
        pass

    def node_deletedself(self, path):
        pass

    def data_changed(self, path):
        pass

    def children_changed(self, path):
        pass


class WatcherWrapper(object):
    def __init__(self, delegate, path):
        assert delegate

        self.delegate = delegate
        self.path = path

    def sessionConnected(self, session_id, session_password, read_only):
        self.delegate.sessionConnected(session_id, session_password, read_only)

    def sessionExpired(self, session_id):
        self.delegate.sessionExpired(session_id)

    def connectionDropped(self):
        self.delegate.connectionDropped()

    def connectionClosed(self):
        self.delegate.connectionClosed()

    def node_created(self, path):
        if self.path == path:
            self.delegate.node_created(path)

    def node_deleted(self, path):
        pass

    def data_changed(self, path):
        pass

    def children_changed(self, path):
        pass


def allocate(hosts, session_id=None, session_passwd=None, session_timeout=30.0, auth_data=None, read_only=False, watcher=None):
    from toolazydogs.zookeeper.zookeeper import Client


    handle = Client(hosts, session_id, session_passwd, session_timeout, auth_data, read_only, watcher)

    return handle


class State(object):
    def __init__(self, code, description):
        self.code = code
        self.description = description


class Connecting(State):
    def __init__(self):
        super(Connecting, self).__init__('CONNECTING', 'Connecting')


class Connected(State):
    def __init__(self):
        super(Connected, self).__init__('CONNECTED', 'Connected')


class ConnectedRO(State):
    def __init__(self):
        super(ConnectedRO, self).__init__('CONNECTED_RO', 'Connected Read-Only')


class AuthFailed(State):
    def __init__(self):
        super(AuthFailed, self).__init__('AUTH_FAILED', 'Authorization Failed')


class Closed(State):
    def __init__(self):
        super(Closed, self).__init__('CLOSED', 'Closed')

CONNECTING = Connecting()
CONNECTED = Connected()
CONNECTED_RO = ConnectedRO()
AUTH_FAILED = AuthFailed()
CLOSED = Closed()

CREATE_CODES = {}

class CreateCode(object):
    def __repr__(self):
        return '%s()' % self.__class__.__name__


def _create_code(name, flags, ephemeral, sequential):
    def decorator(klass):
        def attributes(self, name):
            if name == 'ephemeral': return ephemeral
            if name == 'sequential': return sequential
            if name == 'flags': return flags
            raise AttributeError('Attribute %s not found' % name)

        klass.__getattr__ = attributes

        def string(self):
            return name

        klass.__str__ = string

        CREATE_CODES[flags] = klass()
        return klass

    return decorator


@_create_code('PERSISTENT', 0, False, False)
class Persistent(CreateCode):
    """
    The znode will not be automatically deleted upon client's disconnect.
    """
    pass


@_create_code('EPHEMERAL', 1, True, False)
class Ephemeral(CreateCode):
    """
    The znode will be deleted upon the client's disconnect.
    """
    pass


@_create_code('PERSISTENT_SEQUENTIAL', 2, False, True)
class PersistentSequential(CreateCode):
    """
    The znode will not be automatically deleted upon client's disconnect,
    and its name will be appended with a monotonically increasing number.
    """
    pass


@_create_code('EPHEMERAL_SEQUENTIAL', 3, True, True)
class EphemeralSequential(CreateCode):
    """
    The znode will be deleted upon the client's disconnect, and its name
    will be appended with a monotonically increasing number.

    """
    pass


class Perms(object):
    READ = 1
    WRITE = 2
    CREATE = 4
    DELETE = 8
    ADMIN = 16
    ALL = 31

OPEN_ACL_UNSAFE = [ACL(Perms.ALL, packets.data.Id.ANYONE_ID_UNSAFE)]
CREATOR_ALL_ACL = [ACL(Perms.ALL, packets.data.Id.AUTH_IDS)]
READ_ACL_UNSAFE = [ACL(Perms.READ, packets.data.Id.ANYONE_ID_UNSAFE)]

def _invalid_error_code(): raise RuntimeError('Invalid error code')

EXCEPTIONS = defaultdict(_invalid_error_code)

def _zookeeper_exception(code):
    def decorator(klass):
        def create(*args, **kwargs):
            return klass(args, kwargs)

        EXCEPTIONS[code] = create
        return klass

    return decorator


class ZookeeperError(RuntimeError):
    """ Parent exception for all zookeeper errors """
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

