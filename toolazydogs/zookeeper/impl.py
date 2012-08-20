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
from Queue import Empty, Queue
from exceptions import ValueError
import logging
import random
import select
import struct
import threading
import time
from time import time as _time

from toolazydogs.zookeeper import EXCEPTIONS, CONNECTING, CLOSED, CONNECTED, AuthFailedError, AUTH_FAILED
from toolazydogs.zookeeper.archive import OutputArchive, InputArchive
from toolazydogs.zookeeper.packets.proto.AuthPacket import AuthPacket
from toolazydogs.zookeeper.packets.proto.CloseRequest import CloseRequest
from toolazydogs.zookeeper.packets.proto.CloseResponse import CloseResponse
from toolazydogs.zookeeper.packets.proto.ConnectRequest import ConnectRequest
from toolazydogs.zookeeper.packets.proto.ConnectResponse import ConnectResponse
from toolazydogs.zookeeper.packets.proto.PingRequest import PingRequest
from toolazydogs.zookeeper.packets.proto.ReplyHeader import ReplyHeader
from toolazydogs.zookeeper.packets.proto.WatcherEvent import WatcherEvent


LOGGER = logging.getLogger('toolazydogs.zookeeper.protocol')


class ConnectionDropped(RuntimeError):
    """ Internal error for jumping out of loops """

    def __init__(self, *args, **kwargs):
        super(ConnectionDropped, self).__init__(*args, **kwargs)


class ReaderThread(threading.Thread):
    """ The reader thread

    The reader thread is quite passive, simply reading
    "packets' off the socket and dispatching them.  It
    assumes that the writer thread will perform all the
    cleanup and state orchestration.
    """

    def __init__(self, client, s, reader_started, reader_done, read_timeout):
        super(ReaderThread, self).__init__()
        self.client = client
        self.s = s
        self.reader_started = reader_started
        self.reader_done = reader_done
        self.read_timeout = read_timeout

    def run(self):
        self.reader_started.set()

        while True:
            try:
                header, buffer = _read_header(self.s, self.read_timeout)
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

                    watchers = set()
                    with self.client._state_lock:
                        if watcher_event.type == 1:
                            watchers |= self.client._data_watchers.pop(watcher_event.path, set())
                            watchers |= self.client._exists_watchers.pop(watcher_event.path, set())

                            event = lambda: map(lambda w: w.node_created(watcher_event.path), watchers)
                        elif watcher_event.type == 2:
                            watchers |= self.client._data_watchers.pop(watcher_event.path, set())
                            watchers |= self.client._exists_watchers.pop(watcher_event.path, set())
                            watchers |= self.client._child_watchers.pop(watcher_event.path, set())

                            event = lambda: map(lambda w: w.node_deleted(watcher_event.path), watchers)
                        elif watcher_event.type == 3:
                            watchers |= self.client._data_watchers.pop(watcher_event.path, set())
                            watchers |= self.client._exists_watchers.pop(watcher_event.path, set())

                            event = lambda: map(lambda w: w.data_changed(watcher_event.path), watchers)
                        elif watcher_event.type == 4:
                            watchers |= self.client._child_watchers.pop(watcher_event.path, set())

                            event = lambda: map(lambda w: w.children_changed(watcher_event.path), watchers)
                        else:
                            LOGGER.warn('Received unknown event %r', watcher_event.type)
                            continue

                    self.client._events.put(event)
                else:
                    LOGGER.debug('Reading for header %r', header)

                    request, response, callback, xid = self.client._pending.get()

                    if header.zxid and header.zxid > 0:
                        self.client.last_zxid = header.zxid
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
                        self.s.close()
                        self.reader_done.set()
                        break
            except ConnectionDropped:
                LOGGER.debug('Connection dropped for reader')
                break
            except Exception as e:
                LOGGER.exception(e)
                break

        LOGGER.debug('Reader stopped')


class WriterThread(threading.Thread):
    def __init__(self, client):
        super(WriterThread, self).__init__()
        self.client = client

    def run(self):
        LOGGER.debug('Starting writer')

        writer_done = False

        for host, port in self.client.hosts:
            s = self.client._allocate_socket()

            self.client._state = CONNECTING

            try:
                self._connect(s, host, port)

                reader_started = threading.Event()
                reader_done = threading.Event()

                reader_thread = ReaderThread(self.client, s, reader_started, reader_done, self.read_timeout)
                reader_thread.start()

                reader_started.wait()

                xid = 0
                while not writer_done:
                    try:
                        request, response, callback = self.client._queue.peek(True, self.read_timeout / 2000.0)
                        LOGGER.debug('Sending %r', request)

                        xid += 1
                        LOGGER.debug('xid: %r', xid)

                        _submit(s, request, self.connect_timeout, xid)

                        if isinstance(request, CloseRequest):
                            LOGGER.debug('Received close request, closing')
                            writer_done = True

                        self.client._queue.get()
                        self.client._pending.put((request, response, callback, xid))
                    except Empty:
                        LOGGER.debug('Queue timeout.  Sending PING')
                        _submit(s, PingRequest(), self.connect_timeout, -2)
                    except Exception as e:
                        LOGGER.exception(e)
                        break

                LOGGER.debug('Waiting for reader to read close response')
                reader_done.wait()
                LOGGER.info('Closing connection to %s:%s', host, port)

                if writer_done:
                    self.client._close(CLOSED)
                    break
            except ConnectionDropped:
                LOGGER.warning('Connection dropped')
                self.client._events.put(lambda: self.client._default_watcher.connection_dropped())
                time.sleep(random.random())
            except AuthFailedError:
                LOGGER.warning('AUTH_FAILED closing')
                self.client._close(AUTH_FAILED)
                break
            except Exception as e:
                LOGGER.warning(e)
                time.sleep(random.random())
            finally:
                if not writer_done:
                    # The read thread will close the socket since there
                    # could be a number of pending requests whose response
                    # still needs to be read from the socket.
                    s.close()

        LOGGER.debug('Writer stopped')

    def _connect(self, s, host, port):
        LOGGER.info('Connecting to %s:%s', host, port)
        LOGGER.debug('    Using session_id: %r session_passwd: 0x%s', self.client.session_id, _hex(self.client.session_passwd))

        s.connect((host, port))
        s.setblocking(0)

        LOGGER.debug('Connected')

        connect_request = ConnectRequest(0,
                                         self.client.last_zxid,
                                         int(self.client.session_timeout * 1000),
                                         self.client.session_id or 0,
                                         self.client.session_passwd,
                                         self.client.read_only)
        connection_response = ConnectResponse(None, None, None, None)

        zxid = _invoke(s, self.client.session_timeout, connect_request, connection_response)

        if connection_response.timeOut < 0:
            LOGGER.error('Session expired')
            self.client._events.put(lambda: self.client._default_watcher.session_expired(self.client.session_id))
            raise RuntimeError('Session expired')
        else:
            if zxid: self.client.last_zxid = zxid
            self.client.session_id = connection_response.sessionId
            negotiated_session_timeout = connection_response.timeOut
            self.connect_timeout = negotiated_session_timeout / len(self.client.hosts)
            self.read_timeout = negotiated_session_timeout * 2.0 / 3.0
            self.client.session_passwd = connection_response.passwd
            LOGGER.debug('Session created, session_id: %r session_passwd: 0x%s', self.client.session_id, _hex(self.client.session_passwd))
            LOGGER.debug('    negotiated session timeout: %s', negotiated_session_timeout)
            LOGGER.debug('    connect timeout: %s', self.connect_timeout)
            LOGGER.debug('    read timeout: %s', self.read_timeout)
            self.client._events.put(lambda: self.client._default_watcher.session_connected(self.client.session_id, self.client.session_passwd, self.client.read_only))

        self.client._state = CONNECTED
        connect_failures = 0

        for scheme, auth in self.client.auth_data:
            ap = AuthPacket(0, scheme, auth)
            zxid = _invoke(s, self.connect_timeout, ap, xid=-4)
            if zxid: self.client.last_zxid = zxid


def _invoke(socket, timeout, request, response=None, xid=None):
    oa = OutputArchive()
    if xid:
        oa.write_int(xid, 'xid')
    if request.type:
        oa.write_int(request.type, 'type')
    request.serialize(oa, 'NA')
    socket.send(struct.pack('!i', len(oa.buffer)))
    socket.send(oa.buffer)

    msg = _read(socket, 4, timeout)
    length = struct.unpack_from('!i', msg, 0)[0]

    msg = _read(socket, length, timeout)
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
            callback_exception = EXCEPTIONS[header.err]()
            LOGGER.debug('Received error %r', callback_exception)
            raise callback_exception

    if response:
        response.deserialize(ia, 'NA')
        LOGGER.debug('Read response %r', response)

    return zxid


def _hex(bindata):
    return bindata.encode('hex')


def _submit(socket, request, timeout, xid=None):
    oa = OutputArchive()
    oa.write_int(xid, 'xid')
    if request.type:
        oa.write_int(request.type, 'type')
    request.serialize(oa, 'NA')
    _write(socket, struct.pack('!i', len(oa.buffer)), timeout)
    _write(socket, oa.buffer, timeout)


def _write(socket, buffer, timeout):
    sent = 0
    while sent < len(buffer):
        _, ready_to_write, _ = select.select([], [socket], [], timeout)
        sent = ready_to_write[0].send(buffer[sent:])
        if not sent:
            raise ConnectionDropped('socket connection broken')
        sent = sent + sent


def _read_header(socket, timeout):
    msg = _read(socket, 4, timeout)
    length = struct.unpack_from('!i', msg, 0)[0]

    msg = _read(socket, length, timeout)
    ia = InputArchive(msg)

    header = ReplyHeader(None, None, None)
    header.deserialize(ia, 'header')

    return header, ia


def _read(socket, length, timeout):
    msg = ''
    while len(msg) < length:
        ready_to_read, _, _ = select.select([socket], [], [], timeout)
        chunk = ready_to_read[0].recv(length - len(msg))
        if chunk == '':
            raise ConnectionDropped('socket connection broken')
        msg = msg + chunk
    return msg


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
