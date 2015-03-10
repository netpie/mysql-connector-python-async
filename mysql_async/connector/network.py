# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2012, 2014, Oracle and/or its affiliates. All rights reserved.

# MySQL Connector/Python is licensed under the terms of the GPLv2
# <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
# MySQL Connectors. There are special exceptions to the terms and
# conditions of the GPLv2 as it is applied to this software, see the
# FOSS License Exception
# <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

"""Module implementing low-level socket communication with MySQL servers.
"""

from collections import deque
import socket
import struct
import zlib
import asyncio

from mysql.connector import errors, constants


try:
    import ssl
except:
    # If import fails, we don't have SSL support.
    pass

from mysql.connector.catch23 import PY2, init_bytearray, struct_unpack


def _strioerror(err):
    """Reformat the IOError error message

    This function reformats the IOError error message.
    """
    if not err.errno:
        return str(err)
    return '{errno} {strerr}'.format(errno=err.errno, strerr=err.strerror)


def _prepare_packets(buf, pktnr):
    """Prepare a packet for sending to the MySQL server"""
    pkts = []
    pllen = len(buf)
    maxpktlen = constants.MAX_PACKET_LENGTH
    while pllen > maxpktlen:
        pkts.append(b'\xff\xff\xff' + struct.pack('<B', pktnr)
                    + buf[:maxpktlen])
        buf = buf[maxpktlen:]
        pllen = len(buf)
        pktnr = pktnr + 1
    pkts.append(struct.pack('<I', pllen)[0:3]
                + struct.pack('<B', pktnr) + buf)
    return pkts


class BaseMySQLSocket(object):
    """Base class for MySQL socket communication

    This class should not be used directly but overloaded, changing the
    at least the open_connection()-method. Examples of subclasses are
      mysql.connector.network.MySQLTCPSocket
      mysql.connector.network.MySQLUnixSocket
    """

    def __init__(self, loop=None):
        self._loop = loop
        if loop is None:
            self._loop = asyncio.get_event_loop()
        self._reader = None
        self._writer = None
        self._connection_timeout = None
        self._packet_number = -1
        self._packet_queue = deque()
        self.recvsize = 8192
        self._default_buffer_limit = 2**16

    def set_buffer_limit(self, limit=None):
        if limit is None:
            return self._default_buffer_limit
        if limit > 0:
            self._default_buffer_limit = limit
        else:
            self._default_buffer_limit = 2**16  # default buffer limit passed to StreamReader(self._reader)

    @property
    def next_packet_number(self):
        """Increments the packet number"""
        self._packet_number += 1
        if self._packet_number > 255:
            self._packet_number = 0
        return self._packet_number

    @asyncio.coroutine
    def open_connection(self):
        """Open the socket"""
        raise NotImplementedError

    def get_address(self):
        """Get the location of the socket"""
        raise NotImplementedError

    def shutdown(self):
        """Shut down the socket before closing it"""
        try:
            #self.sock.shutdown(socket.SHUT_RDWR)
            #self.sock.close()
            if not self._writer:
                self._writer.close()
            del self._packet_queue
        except (socket.error, AttributeError):
            pass

    def close_connection(self):
        """Close the socket"""
        try:
            #self.sock.close()
            if not self._writer:
                self._writer.close()
            del self._packet_queue
        except (socket.error, AttributeError):
            pass

    def send_plain(self, buf, packet_number=None):
        """Send packets to the MySQL server"""
        if packet_number is None:
            self.next_packet_number  # pylint: disable=W0104
        else:
            self._packet_number = packet_number
        packets = _prepare_packets(buf, self._packet_number)
        for packet in packets:
            try:
                self._writer.write(packet)
            except IOError as err:
                raise errors.OperationalError(
                    errno=2055, values=(self.get_address(), _strioerror(err)))
            except AttributeError:
                raise errors.OperationalError(errno=2006)

    send = send_plain

    def send_compressed(self, buf, packet_number=None):
        """Send compressed packets to the MySQL server"""
        if packet_number is None:
            self.next_packet_number  # pylint: disable=W0104
        else:
            self._packet_number = packet_number
        pktnr = self._packet_number
        pllen = len(buf)
        zpkts = []
        maxpktlen = constants.MAX_PACKET_LENGTH
        if pllen > maxpktlen:
            pkts = _prepare_packets(buf, pktnr)
            tmpbuf = b''.join(pkts)
            del pkts
            seqid = 0
            zbuf = zlib.compress(tmpbuf[:16384])
            header = (struct.pack('<I', len(zbuf))[0:3]
                      + struct.pack('<B', seqid)
                      + b'\x00\x40\x00')
            zpkts.append(header + zbuf)
            tmpbuf = tmpbuf[16384:]
            pllen = len(tmpbuf)
            seqid = seqid + 1
            while pllen > maxpktlen:
                zbuf = zlib.compress(tmpbuf[:maxpktlen])
                header = (struct.pack('<I', len(zbuf))[0:3]
                          + struct.pack('<B', seqid)
                          + b'\xff\xff\xff')
                zpkts.append(header + zbuf)
                tmpbuf = tmpbuf[maxpktlen:]
                pllen = len(tmpbuf)
                seqid = seqid + 1
            if tmpbuf:
                zbuf = zlib.compress(tmpbuf)
                header = (struct.pack('<I', len(zbuf))[0:3]
                          + struct.pack('<B', seqid)
                          + struct.pack('<I', pllen)[0:3])
                zpkts.append(header + zbuf)
            del tmpbuf
        else:
            pkt = (struct.pack('<I', pllen)[0:3] +
                   struct.pack('<B', pktnr) + buf)
            pllen = len(pkt)
            if pllen > 50:
                zbuf = zlib.compress(pkt)
                zpkts.append(struct.pack('<I', len(zbuf))[0:3]
                             + struct.pack('<B', 0)
                             + struct.pack('<I', pllen)[0:3]
                             + zbuf)
            else:
                header = (struct.pack('<I', pllen)[0:3]
                          + struct.pack('<B', 0)
                          + struct.pack('<I', 0)[0:3])
                zpkts.append(header + pkt)

        for zip_packet in zpkts:
            try:
                #self.sock.sendall(zip_packet)
                self._writer.write(zip_packet)
            except IOError as err:
                raise errors.OperationalError(
                    errno=2055, values=(self.get_address(), _strioerror(err)))
            except AttributeError:
                raise errors.OperationalError(errno=2006)

    @asyncio.coroutine
    def recv_plain(self):
        """Receive packets from the MySQL server"""
        try:
            # Read the header of the MySQL packet, 4 bytes
            packet = yield from self._reader.readexactly(4)

            # Save the packet number and payload length
            self._packet_number = packet[3]
            payload_len = struct.unpack("<I", packet[0:3] + b'\x00')[0]

            # Read the payload
            rest = payload_len
            spacket = packet
            packet = bytearray(4 + payload_len)
            packet[:4] = spacket
            packet_view = memoryview(packet)  # pylint: disable=E0602
            packet_view = packet_view[4:]
            while rest:
                read = yield from self._reader.read(rest)
                lrd = len(read)
                if lrd == 0 and rest > 0:
                    raise errors.InterfaceError(errno=2013)
                packet_view[:lrd] = read
                packet_view = packet_view[lrd:]
                rest -= lrd
            return packet
        except IOError as err:
            raise errors.OperationalError(
                errno=2055, values=(self.get_address(), _strioerror(err)))

    recv = recv_plain

    def _split_zipped_payload(self, packet_bunch):
        """Split compressed payload"""
        while packet_bunch:
            payload_length = struct_unpack("<I",
                                           packet_bunch[0:3] + b'\x00')[0]
            self._packet_queue.append(packet_bunch[0:payload_length + 4])
            packet_bunch = packet_bunch[payload_length + 4:]

    @asyncio.coroutine
    def recv_compressed(self):
        """Receive compressed packets from the MySQL server"""
        try:
            return self._packet_queue.popleft()
        except IndexError:
            pass

        header = bytearray(b'')
        packets = []
        try:
            abyte = yield from self.sock.recv(1)
            while abyte and len(header) < 7:
                header += abyte
                abyte = yield from self.sock.recv(1)
            while header:
                if len(header) < 7:
                    raise errors.InterfaceError(errno=2013)
                zip_payload_length = struct_unpack("<I",
                                                   header[0:3] + b'\x00')[0]
                payload_length = struct_unpack("<I", header[4:7] + b'\x00')[0]
                zip_payload = init_bytearray(abyte)
                while len(zip_payload) < zip_payload_length:
                    chunk = self.sock.recv(zip_payload_length
                                           - len(zip_payload))
                    if len(chunk) == 0:
                        raise errors.InterfaceError(errno=2013)
                    zip_payload = zip_payload + chunk
                if payload_length == 0:
                    self._split_zipped_payload(zip_payload)
                    return self._packet_queue.popleft()
                packets.append(header + zip_payload)
                if payload_length != 16384:
                    break
                header = init_bytearray(b'')
                abyte = yield from self.sock.recv(1)
                while abyte and len(header) < 7:
                    header += abyte
                    abyte = yield from self.sock.recv(1)
        except IOError as err:
            raise errors.OperationalError(
                errno=2055, values=(self.get_address(), _strioerror(err)))

        tmp = init_bytearray(b'')
        for packet in packets:
            payload_length = struct_unpack("<I", header[4:7] + b'\x00')[0]
            if payload_length == 0:
                tmp.append(packet[7:])
            else:
                tmp += zlib.decompress(packet[7:])

        self._split_zipped_payload(tmp)
        del tmp

        try:
            return self._packet_queue.popleft()
        except IndexError:
            pass

    def set_connection_timeout(self, timeout):
        """Set the connection timeout"""
        self._connection_timeout = timeout

    def switch_to_ssl(self, ca, cert, key, verify_cert=False):
        """Switch the socket to use SSL"""
        if not self._writer:
            raise errors.InterfaceError(errno=2048)

        try:
            if verify_cert:
                cert_reqs = ssl.CERT_REQUIRED
            else:
                cert_reqs = ssl.CERT_NONE

            self.sock = ssl.wrap_socket(
                self.sock, keyfile=key, certfile=cert, ca_certs=ca,
                cert_reqs=cert_reqs, do_handshake_on_connect=False,
                ssl_version=ssl.PROTOCOL_TLSv1)
            self.sock.do_handshake()
        except NameError:
            raise errors.NotSupportedError(
                "Python installation has no SSL support")
        except (ssl.SSLError, IOError) as err:
            raise errors.InterfaceError(
                errno=2055, values=(self.get_address(), _strioerror(err)))
        except NotImplementedError as err:
            raise errors.InterfaceError(str(err))

    @asyncio.coroutine
    def drain(self):
        if self._writer is not None:
            yield from self._writer.drain()

class MySQLUnixSocket(BaseMySQLSocket):
    """MySQL socket class using UNIX sockets

    Opens a connection through the UNIX socket of the MySQL Server.
    """

    def __init__(self, unix_socket='/tmp/mysql.sock', loop=None):
        super(MySQLUnixSocket, self).__init__(loop)
        self.unix_socket = unix_socket

    def get_address(self):
        return self.unix_socket

    @asyncio.coroutine
    def open_connection(self):
        try:
            self._reader, self._writer = (yield from asyncio.wait_for(
                asyncio.open_unix_connection(path=self.unix_socket,
                                             loop=self._loop,
                                             limit=self._default_buffer_limit),
                loop=self._loop, timeout=self._connection_timeout))
        except IOError as err:
            raise errors.InterfaceError(
                errno=2002, values=(self.get_address(), _strioerror(err)))
        except Exception as err:
            raise errors.InterfaceError(str(err))


class MySQLTCPSocket(BaseMySQLSocket):
    """MySQL socket class using TCP/IP

    Opens a TCP/IP connection to the MySQL Server.
    """

    def __init__(self, host='127.0.0.1', port=3306, force_ipv6=False, loop=None):
        super(MySQLTCPSocket, self).__init__(loop)
        self.server_host = host
        self.server_port = port
        self.force_ipv6 = force_ipv6
        self._family = 0
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

    def get_address(self):
        return "{0}:{1}".format(self.server_host, self.server_port)

    @asyncio.coroutine
    def open_connection(self):
        """Open the TCP/IP connection to the MySQL server
        """
        # Get address information
        addrinfo = [None] * 5
        try:
            addrinfos = socket.getaddrinfo(self.server_host,
                                           self.server_port,
                                           0, socket.SOCK_STREAM,
                                           socket.SOL_TCP)
            # If multiple results we favor IPv4, unless IPv6 was forced.
            for info in addrinfos:
                if self.force_ipv6 and info[0] == socket.AF_INET6:
                    addrinfo = info
                    break
                elif info[0] == socket.AF_INET:
                    addrinfo = info
                    break
            if self.force_ipv6 and addrinfo[0] is None:
                raise errors.InterfaceError(
                    "No IPv6 address found for {0}".format(self.server_host))
            if addrinfo[0] is None:
                addrinfo = addrinfos[0]
        except IOError as err:
            raise errors.InterfaceError(
                errno=2003, values=(self.get_address(), _strioerror(err)))
        else:
            (self._family, socktype, proto, _, sockaddr) = addrinfo

        # Instanciate the socket and connect
        try:
            self._reader, self._writer = (
                yield from (asyncio.wait_for(asyncio.open_connection(self.server_host,
                                                                     port=self.server_port,
                                                                     loop=self._loop,
                                                                     limit=self._default_buffer_limit),
                                             timeout=self._connection_timeout,
                                             loop=self._loop))
            )
        except IOError as err:
            raise errors.InterfaceError(
                errno=2003, values=(self.get_address(), _strioerror(err)))
        except Exception as err:
            raise errors.OperationalError(str(err))
