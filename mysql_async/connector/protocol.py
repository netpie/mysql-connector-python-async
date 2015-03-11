# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2009, 2014, Oracle and/or its affiliates. All rights reserved.

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

"""Implements the MySQL Client/Server protocol
"""
from mysql.connector import utils
from mysql.connector.protocol import MySQLProtocol
import asyncio


class AioMySQLProtocol(MySQLProtocol):
    """Implements MySQL client/server protocol

    Create and parses MySQL packets.
    """

    @asyncio.coroutine
    def read_text_result(self, sock, count=1):
        """Read MySQL text result

        Reads all or given number of rows from the socket.

        Returns a tuple with 2 elements: a list with all rows and
        the EOF packet.
        """
        rows = []
        eof = None
        rowdata = None
        i = 0
        while True:
            if eof is not None:
                break
            if i == count:
                break
            packet = yield from sock.recv()
            if packet.startswith(b'\xff\xff\xff'):
                datas = [packet[4:]]
                packet = yield from sock.recv()
                while packet.startswith(b'\xff\xff\xff'):
                    datas.append(packet[4:])
                    packet = yield from sock.recv()
                if packet[4] == 254:
                    eof = self.parse_eof(packet)
                else:
                    datas.append(packet[4:])
                rowdata = utils.read_lc_string_list(b''.join(datas))
            elif packet[4] == 254:
                eof = self.parse_eof(packet)
                rowdata = None
            else:
                eof = None
                rowdata = utils.read_lc_string_list(packet[4:])
            if eof is None and rowdata is not None:
                rows.append(rowdata)
            i += 1
        return (rows, eof)

    @asyncio.coroutine
    def read_binary_result(self, sock, columns, count=1):
        """Read MySQL binary protocol result

        Reads all or given number of binary resultset rows from the socket.
        """
        rows = []
        eof = None
        values = None
        i = 0
        while True:
            if eof is not None:
                break
            if i == count:
                break
            packet = yield from sock.recv()
            if packet[4] == 254:
                eof = self.parse_eof(packet)
                values = None
            elif packet[4] == 0:
                eof = None
                values = self._parse_binary_values(columns, packet[5:])
            if eof is None and values is not None:
                rows.append(values)
            i += 1
        return (rows, eof)

