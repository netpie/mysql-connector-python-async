# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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

"""Implementing pooling of connections to MySQL servers.
"""

import re
from uuid import uuid4
# pylint: disable=F0401
try:
    import queue
except ImportError:
    # Python v2
    import Queue as queue
# pylint: enable=F0401
import threading

from mysql.connector import errors
from mysql.connector.pooling import generate_pool_name, PooledMySQLConnection, MySQLConnectionPool, CNX_POOL_ARGS, CNX_POOL_MAXSIZE, CNX_POOL_MAXNAMESIZE, CNX_POOL_NAMEREGEX
from .connection import AioMySQLConnection
import asyncio

CONNECTION_POOL_LOCK = threading.RLock()




class MySQLConPool(MySQLConnectionPool):
    """Class derived MySqlConnectionPool,make for async acesss"""
    def __init__(self, pool_size=5, pool_name=None, pool_reset_session=True, **kwargs):
        """Initialize

        Initialize a MySQL connection pool with a maximum number of
        connections set to pool_size. The rest of the keywords
        arguments, kwargs, are configuration arguments for MySQLConnection
        instances.
        """
        self._pool_size = None
        self._pool_name = None
        self._reset_session = pool_reset_session
        self._set_pool_size(pool_size)
        self._set_pool_name(pool_name or generate_pool_name(**kwargs))
        self._cnx_config = {}
        self._cnx_queue = queue.Queue(self._pool_size)
        self._config_version = uuid4()

        if kwargs:
            self.set_config(**kwargs)
            cnt = 0
            while cnt < self._pool_size:
                self.add_connection()
                cnt += 1

    def add_connection(self, cnx=None):
        """Add a connection to the pool

        This method instantiates a MySQLConnection using the configuration
        passed when initializing the MySQLConnectionPool instance or using
        the set_config() method.
        If cnx is a MySQLConnection instance, it will be added to the
        queue.

        Raises PoolError when no configuration is set, when no more
        connection can be added (maximum reached) or when the connection
        can not be instantiated.
        """
        with CONNECTION_POOL_LOCK:
            if not self._cnx_config:
                raise errors.PoolError(
                    "Connection configuration not available")

            if self._cnx_queue.full():
                raise errors.PoolError(
                    "Failed adding connection; queue is full")

            if not cnx:
                cnx = AioMySQLConnection(**self._cnx_config)
                try:
                    if (self._reset_session and self._cnx_config['compress']
                            and cnx.get_server_version() < (5, 7, 3)):
                        raise errors.NotSupportedError("Pool reset session is "
                                                       "not supported with "
                                                       "compression for MySQL "
                                                       "server version 5.7.2 "
                                                       "or earlier.")
                except KeyError:
                    pass

                # pylint: disable=W0201,W0212
                cnx._pool_config_version = self._config_version
                # pylint: enable=W0201,W0212
            else:
                if not isinstance(cnx, AioMySQLConnection):
                    raise errors.PoolError(
                        "Connection instance not subclass of AioMySQLConnection.")

            self._queue_connection(cnx)

    def set_config(self, **kwargs):
        """Set the connection configuration for MySQLConnection instances

        This method sets the configuration used for creating MySQLConnection
        instances. See MySQLConnection for valid connection arguments.

        Raises PoolError when a connection argument is not valid, missing
        or not supported by MySQLConnection.
        """
        if not kwargs:
            return

        with CONNECTION_POOL_LOCK:
            try:
                test_cnx = AioMySQLConnection()
                test_cnx.config(**kwargs)
                self._cnx_config = kwargs
                self._config_version = uuid4()
            except AttributeError as err:
                raise errors.PoolError(
                    "Connection configuration not valid: {0}".format(err))

    @asyncio.coroutine
    def get_connection(self):
        """Get a connection from the pool

        This method returns an PooledMySQLConnection instance which
        has a reference to the pool that created it, and the next available
        MySQL connection.

        When the MySQL connection is not connect, a reconnect is attempted.

        Raises PoolError on errors.

        Returns a PooledMySQLConnection instance.
        """
        with CONNECTION_POOL_LOCK:
            try:
                cnx = self._cnx_queue.get(block=False)
            except queue.Empty:
                raise errors.PoolError(
                    "Failed getting connection; pool exhausted")

            # pylint: disable=W0201,W0212
            if not (yield from cnx.is_connected()) \
                    or self._config_version != cnx._pool_config_version:
                cnx.config(**self._cnx_config)
                try:
                    yield from cnx.reconnect()
                except errors.InterfaceError:
                    # Failed to reconnect, give connection back to pool
                    self._queue_connection(cnx)
                    raise
                cnx._pool_config_version = self._config_version
            else:
                print("get a pooled connector. current pool size is:", self._cnx_queue.qsize() )

            return PooledMySQLConnection(self, cnx)

