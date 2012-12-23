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

from toolazydogs.pookeeper import EXCEPTIONS, CONNECTING, CLOSED, AuthFailedError, AUTH_FAILED, CONNECTION_DROPPED_FOR_TEST
from toolazydogs.pookeeper.archive import OutputArchive, InputArchive
from toolazydogs.pookeeper.packets.proto.AuthPacket import AuthPacket
from toolazydogs.pookeeper.packets.proto.CloseRequest import CloseRequest
from toolazydogs.pookeeper.packets.proto.CloseResponse import CloseResponse
from toolazydogs.pookeeper.packets.proto.ConnectRequest import ConnectRequest
from toolazydogs.pookeeper.packets.proto.ConnectResponse import ConnectResponse
from toolazydogs.pookeeper.packets.proto.PingRequest import PingRequest
from toolazydogs.pookeeper.packets.proto.ReplyHeader import ReplyHeader
from toolazydogs.pookeeper.packets.proto.WatcherEvent import WatcherEvent


LOGGER = logging.getLogger(__name__)


class ConnectionDropped(RuntimeError):
    """ Internal error for jumping out of loops """

    def __init__(self, *args, **kwargs):
        super(ConnectionDropped, self).__init__(*args, **kwargs)


class SessionTimeout(RuntimeError):
    """ Internal error for jumping out of loops """

    def __init__(self, *args, **kwargs):
        super(SessionTimeout, self).__init__(*args, **kwargs)


class SessionExpired(RuntimeError):
    """ Session expired """

    def __init__(self, *args, **kwargs):
        super(SessionExpired, self).__init__(*args, **kwargs)


class ConnectionDroppedForTest(RuntimeError):
    """ Socket dropped for testing """

    def __init__(self, *args, **kwargs):
        super(ConnectionDroppedForTest, self).__init__(*args, **kwargs)


class ReaderThread(threading.Thread):
    """ The reader thread

    The reader thread is quite passive, simply reading
    "packets' off the socket and dispatching them.  It
    assumes that the writer thread will perform all the
    cleanup and state orchestration.
    """

    def __init__(self, client, s, reader_done, read_timeout):
        super(ReaderThread, self).__init__(name='reader-%s' % client.id)
        self.client = client
        self.s = s
        self.reader_done = reader_done
        self.read_timeout = read_timeout

    def run(self):
        LOGGER.debug('Reader started')
        try:
            while True:
                try:
                    header, input_archive = _read_header_and_body(self.s, self.read_timeout)
                    if header.xid == -2:
                        LOGGER.debug('Received PING')
                        continue
                    elif header.xid == -4:
                        LOGGER.debug('Received AUTH')
                        continue
                    elif header.xid == -1:
                        watcher_event = WatcherEvent(None, None, None)
                        watcher_event.deserialize(input_archive, 'event')

                        path = watcher_event.path
                        watchers = set()
                        with self.client._state_lock:
                            if watcher_event.type == 1:
                                LOGGER.debug('Received created event %s', path)
                                watchers |= self.client._data_watchers.pop(path, set())
                                watchers |= self.client._exists_watchers.pop(path, set())
                                LOGGER.debug(' with %r', watchers)

                                self.client._events.put(_event_factory(path, watchers, lambda w, p: w.node_created(p)))
                            elif watcher_event.type == 2:
                                LOGGER.debug('Received deleted event %s', path)
                                watchers |= self.client._data_watchers.pop(path, set())
                                watchers |= self.client._exists_watchers.pop(path, set())
                                watchers |= self.client._child_watchers.pop(path, set())
                                LOGGER.debug(' with %r', watchers)

                                self.client._events.put(_event_factory(path, watchers, lambda w, p: w.node_deleted(p)))
                            elif watcher_event.type == 3:
                                LOGGER.debug('Received data changed event %s', path)
                                watchers |= self.client._data_watchers.pop(path, set())
                                watchers |= self.client._exists_watchers.pop(path, set())
                                LOGGER.debug(' with %r', watchers)

                                self.client._events.put(_event_factory(path, watchers, lambda w, p: w.data_changed(p)))
                            elif watcher_event.type == 4:
                                LOGGER.debug('Received children changed event %s', path)
                                watchers |= self.client._child_watchers.pop(path, set())
                                LOGGER.debug(' with %r', watchers)

                                self.client._events.put(_event_factory(path, watchers, lambda w, p: w.children_changed(p)))
                            else:
                                LOGGER.warn('Received unknown event %r', watcher_event.type)

                    else:
                        LOGGER.debug('Reading for header %r', header)

                        with self.client._state_lock:
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
                                response.deserialize(input_archive, 'response')
                                LOGGER.debug('Received response: %r', response)

                            try:
                                callback(callback_exception)
                            except Exception as e:
                                LOGGER.exception(e)

                            if isinstance(response, CloseResponse):
                                LOGGER.debug('Read close response')
                                self.s.close()
                                break

                except ConnectionDropped:
                    LOGGER.warning('Connection dropped for reader')
                    raise
                except SessionTimeout:
                    LOGGER.warning('Session timeout for reader')
                    self.s.close()
                    raise
                except Exception as e:
                    LOGGER.exception(e)
                    raise
        except Exception:
            pass
        finally:
            self.reader_done.set()
            LOGGER.debug('Reader stopped')


def _event_factory(path, watchers, callback):
    def event():
        for watcher in watchers:
            try:
                callback(watcher, path)
            except:
                LOGGER.exception()

    return event


class WriterThread(threading.Thread):
    def __init__(self, client):
        super(WriterThread, self).__init__(name='writer-%s' % client.id)
        self.client = client

    def run(self):
        LOGGER.debug('Starting writer %r', self.client.hosts)

        writer_done = False

        succeded_in_connecting = False
        for host, port in self.client.hosts:
            try:
                if succeded_in_connecting and not self.client.allow_reconnect:
                    self.client._closed(CONNECTION_DROPPED_FOR_TEST)
                    break

                self.socket = self.client._allocate_socket()

                self.client._state = CONNECTING

                self._connect(self.socket, host, port)

                succeded_in_connecting = True

                reader_done = threading.Event()

                reader_thread = ReaderThread(self.client, self.socket, reader_done, self.read_timeout)
                reader_thread.start()

                xid = 0
                while not writer_done:
                    try:
                        request, _, _ = self.client._queue.peek(True, self.read_timeout / 2.0)
                        LOGGER.debug('Sending %r', request)

                        xid += 1
                        LOGGER.debug('xid: %r', xid)

                        _submit(self.socket, request, self.connect_timeout, xid)

                        if isinstance(request, CloseRequest):
                            LOGGER.debug('Received close request, closing')
                            writer_done = True

                        # We've successfully sent the packet.  Now we transfer
                        # it to the queue of pending results.
                        with self.client._state_lock:
                            if self.client._queue.peek(block=False):
                                request, response, callback = self.client._queue.get()
                                self.client._pending.put((request, response, callback, xid))
                    except Empty:
                        LOGGER.debug('Queue timeout.  Sending PING')
                        _submit(self.socket, PingRequest(), self.connect_timeout, -2)

                LOGGER.debug('Waiting for reader to read close response')
                reader_done.wait()
                LOGGER.info('Closing connection to %s:%s', host, port)

                if writer_done:
                    self.client._closed(CLOSED)
                    break
            except SessionExpired:
                LOGGER.warning('Session expired, closing')
                self.client._closed(CLOSED, session_expired=True)
                break
            except AuthFailedError:
                LOGGER.warning('Auth failed, closing')
                self.client._closed(AUTH_FAILED)
                break
            except (ConnectionDropped, SessionTimeout, Exception) as e:
                LOGGER.warning(str(e))
                LOGGER.exception(e)
                self.client._disconnected()
                time.sleep(random.random())
            finally:
                if not writer_done:
                    # The read thread will close the socket since there
                    # could be a number of pending requests whose response
                    # still needs to be read from the socket.
                    self.socket.close()

        LOGGER.debug('Writer stopped')

    def _connect(self, s, host, port):
        LOGGER.info('Connecting to %s:%s', host, port)
        LOGGER.debug('    Using session_id: %r session_passwd: 0x%s', self.client.session_id, self.client.session_passwd.encode('hex'))

        s.connect((host, port))
        s.setblocking(0)

        LOGGER.debug('Connected')

        connect_request = ConnectRequest(0,
                                         self.client.last_zxid,
                                         int(self.client.session_timeout * 1000),
                                         self.client.session_id or 0,
                                         self.client.session_passwd,
                                         self.client.read_only)
        connection_response = ConnectResponse(None, None, None, None, None)

        zxid = _invoke(s, self.client.connect_timeout, connect_request, connection_response)

        if connection_response.timeOut < 0:
            LOGGER.error('Session expired')
            self.client._events.put(lambda: self.client._default_watcher.session_expired(self.client.session_id))
            raise SessionExpired()
        else:
            if zxid: self.client.last_zxid = zxid
            self.client.session_id = connection_response.sessionId
            self.client.negotiated_session_timeout = connection_response.timeOut / 1000.0
            self.connect_timeout = connection_response.timeOut / len(self.client.hosts) / 1000.0
            self.read_timeout = connection_response.timeOut * 2.0 / 3.0 / 1000.0
            self.client.session_passwd = connection_response.passwd

            LOGGER.debug('Session created, session_id: %r session_passwd: 0x%s', self.client.session_id, self.client.session_passwd.encode('hex'))
            LOGGER.debug('    negotiated session timeout: %s', self.client.negotiated_session_timeout)
            LOGGER.debug('    connect timeout: %s', self.connect_timeout)
            LOGGER.debug('    read timeout: %s', self.read_timeout)

        self.client._connected(connection_response.sessionId, connection_response.passwd, connection_response.readOnly)

        for scheme, auth in self.client.auth_data:
            ap = AuthPacket(0, scheme, auth)
            zxid = _invoke(s, self.read_timeout, ap, xid=-4)
            if zxid: self.client.last_zxid = zxid


def _invoke(socket, timeout, request, response=None, xid=None):
    oa = OutputArchive()
    if xid:
        oa.write_int(xid, 'xid')
    if request.type:
        oa.write_int(request.type, 'type')
    request.serialize(oa, 'NA')

    timeout = _write(socket, struct.pack('!i', len(oa.buffer)), timeout)
    timeout = _write(socket, oa.buffer, timeout)

    msg, timeout = _read(socket, 4, timeout)
    length = struct.unpack_from('!i', msg, 0)[0]

    msg, _ = _read(socket, length, timeout)
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


def _submit(socket, request, timeout, xid=None):
    oa = OutputArchive()
    oa.write_int(xid, 'xid')
    if request.type:
        oa.write_int(request.type, 'type')
    request.serialize(oa, 'NA')

    timeout = _write(socket, struct.pack('!i', len(oa.buffer)), timeout)
    _write(socket, oa.buffer, timeout)


def _write(socket, buffer, timeout):
    sent = 0
    while sent < len(buffer):
        if timeout <= 0:
            raise SessionTimeout()
        start = time.time()

        _, ready_to_write, _ = select.select([], [socket], [], timeout)
        end = time.time()
        timeout = timeout - (end - start)
        if not ready_to_write:
            raise SessionTimeout()

        count = ready_to_write[0].send(buffer[sent:])
        if not count:
            raise ConnectionDropped()
        sent += count

        return timeout


def _read_header_and_body(socket, timeout):
    msg, timeout = _read(socket, 4, timeout)

    length = struct.unpack_from('!i', msg, 0)[0]

    msg, _ = _read(socket, length, timeout)
    input_archive = InputArchive(msg)

    header = ReplyHeader(None, None, None)
    header.deserialize(input_archive, 'header')

    return header, input_archive


def _read(socket, length, timeout):
    msg = ''
    while len(msg) < length:
        if timeout <= 0:
            raise SessionTimeout()
        start = time.time()

        ready_to_read, _, _ = select.select([socket], [], [], timeout)

        end = time.time()
        timeout = timeout - (end - start)
        if not ready_to_read:
            raise SessionTimeout()

        chunk = ready_to_read[0].recv(length - len(msg))
        if chunk == '':
            raise ConnectionDropped()
        msg = msg + chunk
    return msg, timeout


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
