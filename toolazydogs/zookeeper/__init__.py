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
from collections import defaultdict

from toolazydogs.zookeeper import packets
from toolazydogs.zookeeper.packets.data.ACL import ACL
from toolazydogs.zookeeper.packets.data.Id import Id


__version__ = '1.0.0-dev'

def allocate(hosts, session_id=None, session_passwd=None, session_timeout=30.0, auth_data=None, read_only=False, watcher=None):
    return allocate_34(hosts, session_id, session_passwd, session_timeout, auth_data, read_only, watcher)


def allocate_34(hosts, session_id=None, session_passwd=None, session_timeout=30.0, auth_data=None, read_only=False, watcher=None):
    from toolazydogs.zookeeper.zookeeper import Client34


    handle = Client34(hosts, session_id, session_passwd, session_timeout, auth_data, read_only, watcher)

    return handle


def allocate_33(hosts, session_id=None, session_passwd=None, session_timeout=30.0, auth_data=None, read_only=False, watcher=None):
    from toolazydogs.zookeeper.zookeeper import Client33


    handle = Client33(hosts, session_id, session_passwd, session_timeout, auth_data, read_only, watcher)

    return handle


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

