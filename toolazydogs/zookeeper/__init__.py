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
from Queue import Queue, Empty
from collections import defaultdict

from time import time as _time
from toolazydogs.zookeeper.packets.data.ACL import ACL
from toolazydogs.zookeeper.packets.data.Id import Id

__version__ = '0.1.0'

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


class Watcher(object):
    def sessionConnected(self, session_id, session_password):
        pass

    def sessionExpired(self, session_id):
        pass


def allocate(hosts, session_id=None, session_passwd=None, session_timeout=30.0, auth_data=None, read_only=False):
    from toolazydogs.zookeeper.zookeeper import Client


    handle = Client(hosts, session_id, session_passwd, session_timeout, auth_data, read_only)
    return handle


def _invalid_create_flag(): raise RuntimeError('Invalid create code')

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

ANYONE_ID_UNSAFE = Id('world', 'anyone')
AUTH_IDS = Id('world', 'anyone')

OPEN_ACL_UNSAFE = [ACL(Perms.ALL, ANYONE_ID_UNSAFE)]
CREATOR_ALL_ACL = [ACL(Perms.ALL, AUTH_IDS)]
READ_ACL_UNSAFE = [ACL(Perms.READ, ANYONE_ID_UNSAFE)]

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
class Unimplemented(ZookeeperError):
    pass


@_zookeeper_exception(-7)
class OperationTimeout(ZookeeperError):
    pass


@_zookeeper_exception(-8)
class BadArguments(ZookeeperError):
    pass


@_zookeeper_exception(-100)
class APIError(ZookeeperError):
    pass


@_zookeeper_exception(-101)
class NoNode(ZookeeperError):
    pass


@_zookeeper_exception(-102)
class NoAuth(ZookeeperError):
    pass


@_zookeeper_exception(-103)
class BadVersion(ZookeeperError):
    pass


@_zookeeper_exception(-108)
class NoChildrenForEphemerals(ZookeeperError):
    pass


@_zookeeper_exception(-110)
class NodeExists(ZookeeperError):
    pass


@_zookeeper_exception(-111)
class NotEmpty(ZookeeperError):
    pass


@_zookeeper_exception(-112)
class SessionExpired(ZookeeperError):
    pass


@_zookeeper_exception(-113)
class InvalidCallback(ZookeeperError):
    pass


@_zookeeper_exception(-114)
class InvalidACL(ZookeeperError):
    pass


@_zookeeper_exception(-115)
class AuthFailed(ZookeeperError):
    pass

