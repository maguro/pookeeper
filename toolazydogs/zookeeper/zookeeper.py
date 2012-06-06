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
import logging
import random
import socket
import struct
import threading

from toolazydogs.zookeeper import EXCEPTIONS, NoNode
from toolazydogs.zookeeper.archive import OutputArchive, InputArchive
from toolazydogs.zookeeper.packets.proto.AuthPacket import AuthPacket
from toolazydogs.zookeeper.packets.proto.CloseRequest import CloseRequest
from toolazydogs.zookeeper.packets.proto.CloseResponse import CloseResponse
from toolazydogs.zookeeper.packets.proto.ConnectResponse import ConnectResponse
from toolazydogs.zookeeper.packets.proto.ConnectRequest import ConnectRequest
from toolazydogs.zookeeper.packets.proto.CreateRequest import CreateRequest
from toolazydogs.zookeeper.packets.proto.CreateResponse import CreateResponse
from toolazydogs.zookeeper.packets.proto.DeleteRequest import DeleteRequest
from toolazydogs.zookeeper.packets.proto.ExistsRequest import ExistsRequest
from toolazydogs.zookeeper.packets.proto.ExistsResponse import ExistsResponse
from toolazydogs.zookeeper.packets.proto.GetACLRequest import GetACLRequest
from toolazydogs.zookeeper.packets.proto.GetACLResponse import GetACLResponse
from toolazydogs.zookeeper.packets.proto.GetChildrenRequest import GetChildrenRequest
from toolazydogs.zookeeper.packets.proto.GetChildrenResponse import GetChildrenResponse
from toolazydogs.zookeeper.packets.proto.GetDataRequest import GetDataRequest
from toolazydogs.zookeeper.packets.proto.GetDataResponse import GetDataResponse
from toolazydogs.zookeeper.packets.proto.PingRequest import PingRequest
from toolazydogs.zookeeper.packets.proto.ReplyHeader import ReplyHeader
from toolazydogs.zookeeper.packets.proto.SetACLRequest import SetACLRequest
from toolazydogs.zookeeper.packets.proto.SetACLResponse import SetACLResponse
from toolazydogs.zookeeper.packets.proto.SetDataRequest import SetDataRequest
from toolazydogs.zookeeper.packets.proto.SetDataResponse import SetDataResponse
from toolazydogs.zookeeper.packets.proto.SyncRequest import SyncRequest
from toolazydogs.zookeeper.packets.proto.SyncResponse import SyncResponse
from toolazydogs.zookeeper.packets.proto.WatcherEvent import WatcherEvent


LOGGER = logging.getLogger('toolazydogs.zookeeper')

class ConnectionDropped(RuntimeError):
    """ Internal error for jumping out of loops """

    def __init__(self, *args, **kwargs):
        super(ConnectionDropped, self).__init__(*args, **kwargs)


class Client(object):
    def __init__(self, hosts, session_id=None, session_passwd=None, session_timeout=30.0, auth_data=None, read_only=False):
        from toolazydogs.zookeeper import PeekableQueue


        self.hosts, self.chroot = _collect_hosts(hosts)
        random.shuffle(self.hosts)

        self.session_id = session_id
        self.session_passwd = session_passwd if session_passwd else str(bytearray([0] * 16))
        self.session_timeout = session_timeout
        self.auth_data = auth_data if auth_data else set([])
        self.read_only = read_only

        self.last_zxid = 0

        self._queue = PeekableQueue()
        self._pending = Queue()

        self.watchers = set()
        self._events = Queue()

        def event_worker():
            while True:
                notification = self._events.get()
                for watcher in self.watchers:
                    try:
                        notification(watcher)
                    except Exception as e:
                        LOGGER.exception(e)

        self._event_thread = threading.Thread(target=event_worker)
        self._event_thread.daemon = True
        self._event_thread.start()

        writer_started = threading.Event()

        def writer():
            LOGGER.debug('Starting writer')

            writer_done = False
            reader_done = threading.Event()

            for host, port in self.hosts:
                s = socket.socket()
                try:
                    LOGGER.info('Connecting to %s:%s', host, port)
                    LOGGER.debug('    Using session_id: %r session_passwd: 0x%s', self.session_id, _hex(self.session_passwd))

                    s.connect((host, port))

                    LOGGER.debug('Connected')

                    for scheme, auth in self.auth_data:
                        ap = AuthPacket(0, scheme, auth)
                        zxid = _invoke(s, ap, xid=-4)

                    connect_request = ConnectRequest(0,
                                                     self.last_zxid,
                                                     int(self.session_timeout * 1000),
                                                     self.session_id or 0,
                                                     self.session_passwd,
                                                     self.read_only)
                    connection_response = ConnectResponse(None, None, None, None)

                    zxid = _invoke(s, connect_request, connection_response)

                    if connection_response.timeOut < 0:
                        LOGGER.error('Session expired')
                        self._events.put(lambda w: w.sessionExpired(self.session_id))
                        raise RuntimeError('Session expired')
                    else:
                        if zxid: self.last_zxid = zxid
                        self.session_id = connection_response.sessionId
                        negotiated_session_timeout = connection_response.timeOut
                        self.session_passwd = connection_response.passwd
                        LOGGER.debug('Session created, session_id: %r session_passwd: 0x%s', self.session_id, _hex(self.session_passwd))
                        LOGGER.debug('    negotiated session timeout: %s', negotiated_session_timeout)
                        self._events.put(lambda w: w.sessionConnected(self.session_id, self.session_passwd))

                    reader_started = threading.Event()

                    def reader():
                        reader_started.set()

                        while True:
                            try:
                                header, buffer = _read_header(s)
                                if header.xid == -2:
                                    LOGGER.debug('Received PING')
                                    continue
                                elif header.xid == -4:
                                    LOGGER.debug('Received AUTH')
                                    continue
                                elif header.xid == -1:
                                    LOGGER.debug('Received EVENT')
                                    watcher_event = WatcherEvent(None, None, None)
                                    watcher_event.deserialize(buffer, 'event')
                                    self._events.put(lambda watcher: watcher.event(watcher_event.type, watcher_event.state, watcher_event.path))
                                else:
                                    LOGGER.debug('Reading for header %r', header)

                                    request, response, callback, xid = self._pending.get()

                                    if header.zxid and header.zxid > 0: self.last_zxid = header.zxid
                                    if header.xid != xid:
                                        raise RuntimeError('xids do not match, expected %r received %r', xid, header.xid)

                                    callback_exception = None
                                    if header.err:
                                        callback_exception = EXCEPTIONS[header.err]()
                                        LOGGER.debug('Received error %r', callback_exception)
                                    elif response:
                                        response.deserialize(buffer, 'response')
                                        LOGGER.debug('Received response: %r', response)

                                    try:
                                        callback(callback_exception)
                                    except Exception as e:
                                        LOGGER.exception(e)

                                    if isinstance(response, CloseResponse):
                                        LOGGER.debug('Read close response')
                                        reader_done.set()
                            except ConnectionDropped as ie:
                                LOGGER.debug('Connection dropped for reader')
                                break
                            except Exception as e:
                                LOGGER.exception(e)
                                break

                    reader_thread = threading.Thread(target=reader)
                    reader_thread.start()

                    reader_started.wait()
                    writer_started.set()

                    xid = 0
                    while not writer_done:
                        try:
                            request, response, callback = self._queue.peek(True, negotiated_session_timeout / 2000.0)
                            LOGGER.debug('Sending %r', request)

                            xid = xid + 1
                            LOGGER.debug('xid: %r', xid)

                            _submit(s, request, xid)

                            if isinstance(request, CloseRequest):
                                LOGGER.debug('Received close request, closing')
                                writer_done = True

                            self._queue.get()
                            self._pending.put((request, response, callback, xid))
                        except Empty:
                            LOGGER.debug('Queue timeout.  Sending PING')
                            _submit(s, PingRequest(), -2)
                        except Exception as e:
                            LOGGER.exception(e)
                            break

                    LOGGER.debug('Waiting for reader to read close response')
                    reader_done.wait()
                    LOGGER.info('Closing connection to %s:%s', host, port)

                    s.close()

                    if writer_done:
                        LOGGER.debug('BREAKING')
                        break
                except ConnectionDropped as ie:
                    LOGGER.warning('Connection dropped')
                except Exception as e:
                    LOGGER.exception(e)

        writer_thread = threading.Thread(target=writer)
        writer_thread.start()

        writer_started.wait()

    def close(self):
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
        request = CreateRequest(path, data, acls, code.flags)
        response = CreateResponse(None)

        self._call(request, response)

        return response.path

    def delete(self, path, version):
        request = DeleteRequest(path, version)

        self._call(request, None)

    def exists(self, path, watch=False):
        request = ExistsRequest(path, watch)
        response = ExistsResponse(None)

        try:
            self._call(request, response)
            return response.stat if response.stat.czxid != -1 else None
        except NoNode:
            return None

    def get_data(self, path, watch=False):
        request = GetDataRequest(path, watch)
        response = GetDataResponse(None, None)

        self._call(request, response)

        return response.data, response.stat

    def set_data(self, path, data, version):
        request = SetDataRequest(path, data, version)
        response = SetDataResponse(None)

        self._call(request, response)

        return response.stat

    def get_acls(self, path):
        request = GetACLRequest(path)
        response = GetACLResponse(None, None)

        self._call(request, response)

        return response.acl, response.stat

    def set_acls(self, path, acls, version):
        request = SetACLRequest(path, acls, version)
        response = SetACLResponse(None)

        self._call(request, response)

        return response.stat

    def sync(self, path):
        request = SyncRequest(path)
        response = SyncResponse(None)

        self._call(request, response)

    def get_children(self, path, watch=False):
        request = GetChildrenRequest(path, watch)
        response = GetChildrenResponse(None)

        self._call(request, response)

        return response.children

    def _call(self, request, response):
        call_exception = [None]
        event = threading.Event()

        def callback(exception):
            call_exception[0] = exception
            event.set()

        self._queue.put((request, response, callback))
        event.wait()
        if call_exception[0]:
            raise call_exception[0]


def _invoke(socket, request, response=None, xid=None):
    oa = OutputArchive()
    if xid:
        oa.write_int(xid, 'xid')
    if request.type:
        oa.write_int(request.type, 'type')
    request.serialize(oa, 'NA')
    socket.send(struct.pack('!i', len(oa.buffer)))
    socket.send(oa.buffer)

    msg = _read(socket, 4)
    length = struct.unpack_from('!i', msg, 0)[0]

    msg = _read(socket, length)
    ia = InputArchive(msg)

    zxid = None
    if xid:
        header = ReplyHeader(None, None, None)
        header.deserialize(ia, 'header')
        if header.xid != xid:
            raise RuntimeError('xids do not match, expected %r received %r', xid, header.xid)
        if header.zxid > 0:
            zxid = header.zxid
        if header.err:
            pass

    if response:
        response.deserialize(ia, 'NA')
        LOGGER.debug('Read response %r', response)

    return zxid


def _submit(socket, request, xid=None):
    oa = OutputArchive()
    if xid:
        oa.write_int(xid, 'xid')
    if request.type:
        oa.write_int(request.type, 'type')
    request.serialize(oa, 'NA')
    socket.send(struct.pack('!i', len(oa.buffer)))
    socket.send(oa.buffer)


def _read_header(socket):
    msg = _read(socket, 4)
    length = struct.unpack_from('!i', msg, 0)[0]

    msg = _read(socket, length)
    ia = InputArchive(msg)

    header = ReplyHeader(None, None, None)
    header.deserialize(ia, 'header')

    return header, ia


def _read(socket, length):
    msg = ''
    while len(msg) < length:
        chunk = socket.recv(length - len(msg))
        if chunk == '':
            raise ConnectionDropped("socket connection broken")
        msg = msg + chunk
    return msg


def _collect_hosts(hosts):
    index = hosts.find('/')
    if index > 0:
        host_ports = hosts[:index]
        chroot = hosts[index:]
    else:
        host_ports = hosts
        chroot = None

    x = []
    if host_ports.find(',') > 0:
        x.extend(host_ports.split(','))
    else:
        x.append(host_ports)

    result = []
    for host_port in x:
        index = host_port.find(':')
        if index > 0:
            port = int(host_port[index + 1:])
            host = host_port[:index]
        else:
            host = host_port
            port = 2181
        result.append((host.strip(), port))

    return (result, chroot)


def _hex(bindata):
    return bindata.encode('hex')