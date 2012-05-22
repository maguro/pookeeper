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

    def __init__(self, *args, **kwargs):
        super(ZookeeperError, self).__init__(*args, **kwargs)


@_zookeeper_exception(-1)
class SystemZookeeperError(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(SystemZookeeperError, self).__init__(*args, **kwargs)


@_zookeeper_exception(-2)
class RuntimeInconsistency(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(RuntimeInconsistency, self).__init__(*args, **kwargs)


@_zookeeper_exception(-3)
class DataInconsistency(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(DataInconsistency, self).__init__(*args, **kwargs)


@_zookeeper_exception(-4)
class ConnectionLoss(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(ConnectionLoss, self).__init__(*args, **kwargs)


@_zookeeper_exception(-5)
class MarshallingError(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(MarshallingError, self).__init__(*args, **kwargs)


@_zookeeper_exception(-6)
class Unimplemented(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(Unimplemented, self).__init__(*args, **kwargs)


@_zookeeper_exception(-7)
class OperationTimeout(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(OperationTimeout, self).__init__(*args, **kwargs)


@_zookeeper_exception(-8)
class BadArguments(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(BadArguments, self).__init__(*args, **kwargs)


@_zookeeper_exception(-100)
class APIError(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(APIError, self).__init__(*args, **kwargs)


@_zookeeper_exception(-101)
class NoNode(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(NoNode, self).__init__(*args, **kwargs)


@_zookeeper_exception(-102)
class NoAuth(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(NoAuth, self).__init__(*args, **kwargs)


@_zookeeper_exception(-103)
class BadVersion(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(BadVersion, self).__init__(*args, **kwargs)

@_zookeeper_exception(-108)
class NoChildrenForEphemerals(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(NoChildrenForEphemerals, self).__init__(*args, **kwargs)

@_zookeeper_exception(-110)
class NodeExists(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(NodeExists, self).__init__(*args, **kwargs)

@_zookeeper_exception(-111)
class NotEmpty(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(NotEmpty, self).__init__(*args, **kwargs)

@_zookeeper_exception(-112)
class SessionExpired(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(SessionExpired, self).__init__(*args, **kwargs)

@_zookeeper_exception(-113)
class InvalidCallback(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(InvalidCallback, self).__init__(*args, **kwargs)

@_zookeeper_exception(-114)
class InvalidACL(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(InvalidACL, self).__init__(*args, **kwargs)

@_zookeeper_exception(-115)
class AuthFailed(ZookeeperError):
    def __init__(self, *args, **kwargs):
        super(AuthFailed, self).__init__(*args, **kwargs)

