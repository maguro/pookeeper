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
import logging
import select
import struct

from toolazydogs.zookeeper.archive import OutputArchive, InputArchive
from toolazydogs.zookeeper.packets.proto.ReplyHeader import ReplyHeader


LOGGER = logging.getLogger('toolazydogs.zookeeper.protocol')


class ConnectionDropped(RuntimeError):
    """ Internal error for jumping out of loops """

    def __init__(self, *args, **kwargs):
        super(ConnectionDropped, self).__init__(*args, **kwargs)


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
            pass

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
    _write(socket, struct.pack('!i', len(oa.buffer)), timeout)
    _write(socket, oa.buffer, timeout)


def _write(socket, buffer, timeout):
    sent = 0
    while sent < len(buffer):
        _, ready_to_write, _ = select.select([], [socket], [], timeout)
        sent = ready_to_write[0].send(buffer[sent:])
        if sent == 0:
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
