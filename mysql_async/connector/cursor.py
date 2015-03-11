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

"""Cursor classes
"""

from collections import namedtuple
import re
import asyncio

from mysql.connector import errors
from mysql.connector.cursor import (
    MySQLCursor, SQL_COMMENT, RE_SQL_COMMENT, RE_SQL_ON_DUPLICATE, RE_SQL_INSERT_STMT,
    RE_SQL_INSERT_VALUES, RE_PY_PARAM, RE_SQL_SPLIT_STMTS, RE_SQL_FIND_PARAM,
    _ERR_NO_RESULT_TO_FETCH, _ParamSubstitutor)


class AioMySQLCursor(MySQLCursor):
    """Default cursor for interacting with MySQL

    This cursor will execute statements and handle the result. It will
    not automatically fetch all rows.

    MySQLCursor should be inherited whenever other functionallity is
    required. An example would to change the fetch* member functions
    to return dictionaries instead of lists of values.

    Implements the Python Database API Specification v2.0 (PEP-249)
    """
    def __init__(self, connection=None):
        super(AioMySQLCursor, self).__init__(connection=connection)

    def __iter__(self):
        """
        Iteration over the result set which calls self.fetchone()
        and returns the next row.
        """
        return iter(self.fetchone, None)

    @asyncio.coroutine
    def next(self):
        """Used for iterating over the result set."""
        return self.__next__()

    @asyncio.coroutine
    def __next__(self):
        """
        Used for iterating over the result set. Calles self.fetchone()
        to get the next row.
        """
        try:
            row = yield from self.fetchone()
        except errors.InterfaceError:
            raise StopIteration
        if not row:
            raise StopIteration
        return row

    @asyncio.coroutine
    def close(self):
        """Close the cursor

        Returns True when successful, otherwise False.
        """
        if self._connection is None:
            return False
        if self._have_unread_result():
            raise errors.InternalError("Unread result found.")

        self._reset_result()
        self._executed_list = []
        self._connection = None

        return True

    @asyncio.coroutine
    def _handle_noresultset(self, res):
        """Handles result of execute() when there is no result set
        """
        try:
            self._rowcount = res['affected_rows']
            self._last_insert_id = res['insert_id']
            self._warning_count = res['warning_count']
        except (KeyError, TypeError) as err:
            raise errors.ProgrammingError(
                "Failed handling non-resultset; {0}".format(err))

        if self._connection.get_warnings is True and self._warning_count:
            self._warnings = yield from self._fetch_warnings()

    @asyncio.coroutine
    def _handle_resultset(self):
        """Handles result set

        This method handles the result set and is called after reading
        and storing column information in _handle_result(). For non-buffering
        cursors, this method is usually doing nothing.
        """
        pass

    @asyncio.coroutine
    def _handle_result(self, result):
        """
        Handle the result after a command was send. The result can be either
        an OK-packet or a dictionary containing column/eof information.

        Raises InterfaceError when result is not a dict() or result is
        invalid.
        """
        if not isinstance(result, dict):
            raise errors.InterfaceError('Result was not a dict()')

        if 'columns' in result:
            # Weak test, must be column/eof information
            self._description = result['columns']
            self._connection.unread_result = True
            yield from self._handle_resultset()
        elif 'affected_rows' in result:
            # Weak test, must be an OK-packet
            self._connection.unread_result = False
            yield from self._handle_noresultset(result)
        else:
            raise errors.InterfaceError('Invalid result')

    @asyncio.coroutine
    def next_exec_result(self):
        rd = yield from self._connection.next_result()
        if rd is None:
            self._executed_list = []
            return False
        else:
            #ostmt = self._executed_list
            self._reset_result()
            yield from self._handle_result(rd)
            self._executed_list = self._executed_list[1:]
            self._executed = self._executed_list[0]
            return True

    def _reset_result(self):
        """Reset the cursor to default"""
        self._rowcount = -1
        self._nextrow = (None, None)
        self._stored_results = []
        self._warnings = None
        self._warning_count = 0
        self._description = None
        self._executed = None
        #self._executed_list = []
        self.reset()

    @asyncio.coroutine
    def _execute_many(self, query_first):
        """Generator returns MySQLCursor objects for multiple statements

        This method is only used when multiple statements are executed
        by the execute() method. It uses zip() to make an iterator from the
        given query_iter (result of MySQLConnection.cmd_query_iter()) and
        the list of statements that were executed.
        """
        self._reset_result()
        self._executed = self._executed_list[0].strip()
        yield from self._handle_result(query_first)

        return self

    @asyncio.coroutine
    def execute(self, operation, params=None, multi=False):
        """Executes the given operation

        Executes the given operation substituting any markers with
        the given parameters.

        For example, getting all rows where id is 5:
          cursor.execute("SELECT * FROM t1 WHERE id = %s", (5,))

        The multi argument should be set to True when executing multiple
        statements in one operation. If not set and multiple results are
        found, an InterfaceError will be raised.

        If warnings where generated, and connection.get_warnings is True, then
        self._warnings will be a list containing these warnings.

        Returns an iterator when multi is True, otherwise None.
        """
        if not operation:
            return None

        if not self._connection:
            raise errors.ProgrammingError("Cursor is not connected.")
        if self._connection.unread_result is True:
            raise errors.InternalError("Unread result found.")

        self._reset_result()
        self._executed_list = []
        stmt = ''

        try:
            if not isinstance(operation, (bytes, bytearray)):
                stmt = operation.encode(self._connection.python_charset)
            else:
                stmt = operation
        except (UnicodeDecodeError, UnicodeEncodeError) as err:
            raise errors.ProgrammingError(str(err))

        if params is not None:
            if isinstance(params, dict):
                for key, value in self._process_params_dict(params).items():
                    stmt = stmt.replace(key, value)
            elif isinstance(params, (list, tuple)):
                psub = _ParamSubstitutor(self._process_params(params))
                stmt = RE_PY_PARAM.sub(psub, stmt)
                if psub.remaining != 0:
                    raise errors.ProgrammingError(
                        "Not all parameters were used in the SQL statement")

        if multi:
            self._executed = stmt
            self._executed_list = RE_SQL_SPLIT_STMTS.split(self._executed)
            #return self._execute_iter(self._connection.cmd_query_iter(stmt))
            return (yield from self._execute_many((yield from self._connection.cmd_query_many(stmt))))
        else:
            self._executed = stmt
            try:
                yield from self._handle_result((yield from self._connection.cmd_query(stmt)))
            except errors.InterfaceError:
                if self._connection._have_next_result:  # pylint: disable=W0212
                    raise errors.InterfaceError(
                        "Use multi=True when executing multiple statements")
                raise
            return None

    def _batch_insert(self, operation, seq_params):
        """Implements multi row insert"""
        def remove_comments(match):
            """Remove comments from INSERT statements.

            This function is used while removing comments from INSERT
            statements. If the matched string is a comment not enclosed
            by quotes, it returns an empty string, else the string itself.
            """
            if match.group(1):
                return ""
            else:
                return match.group(2)

        tmp = re.sub(RE_SQL_ON_DUPLICATE, '',
                     re.sub(RE_SQL_COMMENT, remove_comments, operation))

        matches = re.search(RE_SQL_INSERT_VALUES, tmp)
        if not matches:
            raise errors.InterfaceError(
                "Failed rewriting statement for multi-row INSERT. "
                "Check SQL syntax."
            )
        fmt = matches.group(1).encode(self._connection.charset)
        values = []

        try:
            stmt = operation.encode(self._connection.charset)
            for params in seq_params:
                tmp = fmt
                if isinstance(params, dict):
                    for key, value in self._process_params_dict(params).items():
                        tmp = tmp.replace(key, value)
                else:
                    psub = _ParamSubstitutor(self._process_params(params))
                    tmp = RE_PY_PARAM.sub(psub, tmp)
                    if psub.remaining != 0:
                        raise errors.ProgrammingError(
                            "Not all parameters were used in the SQL statement")
                    #for p in self._process_params(params):
                    #    tmp = tmp.replace(b'%s',p,1)
                values.append(tmp)
            if fmt in stmt:
                stmt = stmt.replace(fmt, b','.join(values), 1)
                self._executed = stmt
                return stmt
            else:
                return None
        except (UnicodeDecodeError, UnicodeEncodeError) as err:
            raise errors.ProgrammingError(str(err))
        except errors.Error:
            raise
        except Exception as err:
            raise errors.InterfaceError(
                "Failed executing the operation; %s" % err)

    @asyncio.coroutine
    def executemany(self, operation, seq_params):
        """Execute the given operation multiple times

        The executemany() method will execute the operation iterating
        over the list of parameters in seq_params.

        Example: Inserting 3 new employees and their phone number

        data = [
            ('Jane','555-001'),
            ('Joe', '555-001'),
            ('John', '555-003')
            ]
        stmt = "INSERT INTO employees (name, phone) VALUES ('%s','%s)"
        cursor.executemany(stmt, data)

        INSERT statements are optimized by batching the data, that is
        using the MySQL multiple rows syntax.

        Results are discarded. If they are needed, consider looping over
        data using the execute() method.
        """
        if not operation or not seq_params:
            return None
        if self._connection.unread_result is True:
            raise errors.InternalError("Unread result found.")
        if not isinstance(seq_params, (list, tuple)):
            raise errors.ProgrammingError(
                "Parameters for query must be list or tuple.")
        # Optimize INSERTs by batching them
        if re.match(RE_SQL_INSERT_STMT, operation):
            if not seq_params:
                self._rowcount = 0
                return
            stmt = self._batch_insert(operation, seq_params)
            if stmt is not None:
                return (yield from self.execute(stmt))
        rowcnt = 0
        try:
            for params in seq_params:
                yield from self.execute(operation, params)
                if self.with_rows and self._have_unread_result():
                    yield from self.fetchall()
                rowcnt += self._rowcount
        except (ValueError, TypeError) as err:
            raise errors.InterfaceError(
                "Failed executing the operation; {0}".format(err))
        except:
            # Raise whatever execute() raises
            raise
        self._rowcount = rowcnt

    def stored_results(self):
        """Returns an iterator for stored results

        This method returns an iterator over results which are stored when
        callproc() is called. The iterator will provide MySQLCursorBuffered
        instances.

        Returns a iterator.
        """
        return iter(self._stored_results)

    @asyncio.coroutine
    def callproc(self, procname, args=()):
        """Calls a stored procedure with the given arguments

        The arguments will be set during this session, meaning
        they will be called like  _<procname>__arg<nr> where
        <nr> is an enumeration (+1) of the arguments.

        Coding Example:
          1) Defining the Stored Routine in MySQL:
          CREATE PROCEDURE multiply(IN pFac1 INT, IN pFac2 INT, OUT pProd INT)
          BEGIN
            SET pProd := pFac1 * pFac2;
          END

          2) Executing in Python:
          args = (5, 5, 0)  # 0 is to hold pprod
          cursor.callproc('multiply', args)
          print(cursor.fetchone())

        For OUT and INOUT parameters the user should provide the
        type of the parameter as well. The argument should be a
        tuple with first item as the value of the parameter to pass
        and second argument the type of the argument.

        In the above example, one can call callproc method like:
          args = (5, 5, (0, 'INT'))
          cursor.callproc('multiply', args)

        The type of the argument given in the tuple will be used by
        the MySQL CAST function to convert the values in the corresponding
        MySQL type (See CAST in MySQL Reference for more information)

        Does not return a value, but a result set will be
        available when the CALL-statement execute successfully.
        Raises exceptions when something is wrong.
        """
        if not procname or not isinstance(procname, str):
            raise ValueError("procname must be a string")

        if not isinstance(args, (tuple, list)):
            raise ValueError("args must be a sequence")

        argfmt = "@_{name}_arg{index}"
        self._stored_results = []

        results = []
        try:
            argnames = []
            argtypes = []
            if args:
                for idx, arg in enumerate(args):
                    argname = argfmt.format(name=procname, index=idx + 1)
                    argnames.append(argname)
                    if isinstance(arg, tuple):
                        argtypes.append(" CAST({0} AS {1})".format(argname,
                                                                   arg[1]))
                        yield from self.execute("SET {0}=%s".format(argname), (arg[0],))
                    else:
                        argtypes.append(argname)
                        yield from self.execute("SET {0}=%s".format(argname), (arg,))

            call = "CALL {0}({1})".format(procname, ','.join(argnames))

            for result in self._connection.cmd_query_iter(call):
                if 'columns' in result:
                    # pylint: disable=W0212
                    tmp = MySQLCursorBuffered(self._connection._get_self())
                    yield from tmp._handle_result(result)
                    results.append(tmp)
                    # pylint: enable=W0212

            if argnames:
                select = "SELECT {0}".format(','.join(argtypes))
                yield from self.execute(select)
                self._stored_results = results
                return self.fetchone()
            else:
                self._stored_results = results
                return ()

        except errors.Error:
            raise
        except Exception as err:
            raise errors.InterfaceError(
                "Failed calling stored routine; {0}".format(err))

    @asyncio.coroutine
    def _fetch_warnings(self):
        """
        Fetch warnings doing a SHOW WARNINGS. Can be called after getting
        the result.

        Returns a result set or None when there were no warnings.
        """
        res = []
        try:
            cur = yield from self._connection.cursor()
            yield from cur.execute("SHOW WARNINGS")
            res = yield from cur.fetchall()
            cur.close()
        except Exception as err:
            raise errors.InterfaceError(
                "Failed getting warnings; %s" % err)

        if self._connection.raise_on_warnings is True:
            raise errors.get_mysql_exception(res[0][1], res[0][2])
        else:
            if len(res):
                return res

        return None

    @asyncio.coroutine
    def _handle_eof(self, eof):
        """Handle EOF packet"""
        self._connection.unread_result = False
        self._nextrow = (None, None)
        self._warning_count = eof['warning_count']
        if self._connection.get_warnings is True and eof['warning_count']:
            self._warnings = yield from self._fetch_warnings()

    @asyncio.coroutine
    def _fetch_row(self):
        """Returns the next row in the result set

        Returns a tuple or None.
        """
        if not self._have_unread_result():
            return None
        row = None

        if self._nextrow == (None, None):
            (row, eof) = yield from self._connection.get_row(
                binary=self._binary, columns=self.description)
        else:
            (row, eof) = self._nextrow

        if row:
            self._nextrow = yield from self._connection.get_row(
                binary=self._binary, columns=self.description)
            eof = self._nextrow[1]
            if eof is not None:
                yield from self._handle_eof(eof)
            if self._rowcount == -1:
                self._rowcount = 1
            else:
                self._rowcount += 1
        if eof:
            yield from self._handle_eof(eof)

        return row

    @asyncio.coroutine
    def fetchone(self):
        """Returns next row of a query result set

        Returns a tuple or None.
        """
        row = yield from self._fetch_row()
        if row:
            return self._connection.converter.row_to_python(
                row, self.description)
        return None

    @asyncio.coroutine
    def fetchmany(self, size=None):
        res = []
        cnt = (size or self.arraysize)
        while cnt > 0 and self._have_unread_result():
            cnt -= 1
            row = yield from self.fetchone()
            if row:
                res.append(row)
        return res

    @asyncio.coroutine
    def fetchall(self):
        if not self._have_unread_result():
            raise errors.InterfaceError("No result set to fetch from.")
        (rows, eof) = yield from self._connection.get_rows()
        if self._nextrow[0]:
            rows.insert(0, self._nextrow[0])
        res = [self._connection.converter.row_to_python(row, self.description)
               for row in rows]
        yield from self._handle_eof(eof)
        rowcount = len(rows)
        if rowcount >= 0 and self._rowcount == -1:
            self._rowcount = 0
        self._rowcount += rowcount
        return res

    def __str__(self):
        fmt = "MySQLCursor: %s"
        if self._executed:
            executed = bytearray(self._executed).decode('utf-8')
            if len(executed) > 30:
                res = fmt % (executed[:30] + '..')
            else:
                res = fmt % (executed)
        else:
            res = fmt % '(Nothing executed yet)'
        return res


class AioMySQLCursorBuffered(AioMySQLCursor):
    """Cursor which fetches rows within execute()"""

    def __init__(self, connection=None):
        AioMySQLCursor.__init__(self, connection)
        self._rows = None
        self._next_row = 0

    @asyncio.coroutine
    def _handle_resultset(self):
        (self._rows, eof) = yield from self._connection.get_rows()
        self._rowcount = len(self._rows)
        yield from self._handle_eof(eof)
        self._next_row = 0
        try:
            self._connection.unread_result = False
        except:
            pass

    def reset(self):
        self._rows = None

    @asyncio.coroutine
    def _fetch_row(self):
        row = None
        try:
            row = self._rows[self._next_row]
        except:
            return None
        else:
            self._next_row += 1
            return row
        return None

    @asyncio.coroutine
    def fetchall(self):
        if self._rows is None:
            raise errors.InterfaceError("No result set to fetch from.")
        res = []
        for row in self._rows[self._next_row:]:
            res.append(self._connection.converter.row_to_python(
                row, self.description))
        self._next_row = len(self._rows)
        return res

    @asyncio.coroutine
    def fetchmany(self, size=None):
        res = []
        cnt = (size or self.arraysize)
        while cnt > 0:
            cnt -= 1
            row = yield from self.fetchone()
            if row:
                res.append(row)

        return res

    @property
    def with_rows(self):
        return self._rows is not None


class AioMySQLCursorRaw(AioMySQLCursor):
    """
    Skips conversion from MySQL datatypes to Python types when fetching rows.
    """
    @asyncio.coroutine
    def fetchone(self):
        row = yield from self._fetch_row()
        if row:
            return row
        return None

    @asyncio.coroutine
    def fetchall(self):
        if not self._have_unread_result():
            raise errors.InterfaceError("No result set to fetch from.")
        (rows, eof) = yield from self._connection.get_rows()
        if self._nextrow[0]:
            rows.insert(0, self._nextrow[0])
        yield from self._handle_eof(eof)
        rowcount = len(rows)
        if rowcount >= 0 and self._rowcount == -1:
            self._rowcount = 0
        self._rowcount += rowcount
        return rows


class AioMySQLCursorBufferedRaw(AioMySQLCursorBuffered):
    """
    Cursor which skips conversion from MySQL datatypes to Python types when
    fetching rows and fetches rows within execute().
    """
    @asyncio.coroutine
    def fetchone(self):
        row = yield from self._fetch_row()
        if row:
            return row
        return None

    @asyncio.coroutine
    def fetchall(self):
        if self._rows is None:
            raise errors.InterfaceError("No result set to fetch from.")
        return [r for r in self._rows[self._next_row:]]

    @property
    def with_rows(self):
        return self._rows is not None


class AioMySQLCursorPrepared(AioMySQLCursor):
    """Cursor using MySQL Prepared Statements
    """
    def __init__(self, connection=None):
        super(AioMySQLCursorPrepared, self).__init__(connection)
        self._rows = None
        self._next_row = 0
        self._prepared = None
        self._binary = True
        self._have_result = None

    @asyncio.coroutine
    def callproc(self, *args, **kwargs):
        """Calls a stored procedue

        Not supported with MySQLCursorPrepared.
        """
        raise errors.NotSupportedError()

    @asyncio.coroutine
    def close(self):
        """Close the cursor

        This method will try to deallocate the prepared statement and close
        the cursor.
        """
        if self._prepared:
            try:
                yield from self._connection.cmd_stmt_close(self._prepared['statement_id'])
            except errors.Error:
                # We tried to deallocate, but it's OK when we fail.
                pass
            self._prepared = None
        yield from super(AioMySQLCursorPrepared, self).close()

    def _row_to_python(self, rowdata, desc=None):
        """Convert row data from MySQL to Python types

        The conversion is done while reading binary data in the
        protocol module.
        """
        pass

    @asyncio.coroutine
    def _handle_result(self, res):
        """Handle result after execution"""
        if isinstance(res, dict):
            self._connection.unread_result = False
            self._have_result = False
            yield from self._handle_noresultset(res)
        else:
            self._description = res[1]
            self._connection.unread_result = True
            self._have_result = True

    @asyncio.coroutine
    def execute(self, operation, params=(), multi=False):  # multi is unused
        """Prepare and execute a MySQL Prepared Statement

        This method will preare the given operation and execute it using
        the optionally given parameters.

        If the cursor instance already had a prepared statement, it is
        first closed.
        """
        if operation is not self._executed:
            if self._prepared:
                yield from self._connection.cmd_stmt_close(self._prepared['statement_id'])

            self._executed = operation
            try:
                if not isinstance(operation, bytes):
                    operation = operation.encode(self._connection.charset)
            except (UnicodeDecodeError, UnicodeEncodeError) as err:
                raise errors.ProgrammingError(str(err))

            # need to convert %s to ? before sending it to MySQL
            if b'%s' in operation:
                operation = re.sub(RE_SQL_FIND_PARAM, b'?', operation)

            try:
                self._prepared = yield from self._connection.cmd_stmt_prepare(operation)
            except errors.Error:
                self._executed = None
                raise

        yield from self._connection.cmd_stmt_reset(self._prepared['statement_id'])

        if self._prepared['parameters'] and not params:
            return
        elif len(self._prepared['parameters']) != len(params):
            raise errors.ProgrammingError(
                errno=1210,
                msg="Incorrect number of arguments " \
                    "executing prepared statement")

        res = yield from self._connection.cmd_stmt_execute(
            self._prepared['statement_id'],
            data=params,
            parameters=self._prepared['parameters'])
        yield from self._handle_result(res)

    @asyncio.coroutine
    def executemany(self, operation, seq_params):
        """Prepare and execute a MySQL Prepared Statement many times

        This method will prepare the given operation and execute with each
        tuple found the list seq_params.

        If the cursor instance already had a prepared statement, it is
        first closed.

        executemany() simply calls execute().
        """
        rowcnt = 0
        try:
            for params in seq_params:
                yield from self.execute(operation, params)
                if self.with_rows and self._have_unread_result():
                    self.fetchall()
                rowcnt += self._rowcount
        except (ValueError, TypeError) as err:
            raise errors.InterfaceError(
                "Failed executing the operation; {error}".format(error=err))
        except:
            # Raise whatever execute() raises
            raise
        self._rowcount = rowcnt

    @asyncio.coroutine
    def fetchone(self):
        """Returns next row of a query result set

        Returns a tuple or None.
        """
        rd = yield from self._fetch_row()
        return rd or None

    @asyncio.coroutine
    def fetchmany(self, size=None):
        res = []
        cnt = (size or self.arraysize)
        while cnt > 0 and self._have_unread_result():
            cnt -= 1
            row = yield from self._fetch_row()
            if row:
                res.append(row)
        return res

    @asyncio.coroutine
    def fetchall(self):
        if not self._have_unread_result():
            raise errors.InterfaceError("No result set to fetch from.")
        (rows, eof) = yield from self._connection.get_rows(
            binary=self._binary, columns=self.description)
        self._rowcount = len(rows)
        yield from self._handle_eof(eof)
        return rows


class AioMySQLCursorDict(AioMySQLCursor):
    """
    Cursor fetching rows as dictionaries.

    The fetch methods of this class will return dictionaries instead of tuples.
    Each row is a dictionary that looks like:
        row = {
            "col1": value1,
            "col2": value2
        }
    """
    def _row_to_python(self, rowdata, desc=None):
        """Convert a MySQL text result row to Python types

        Returns a dictionary.
        """
        row = self._connection.converter.row_to_python(rowdata, desc)
        if row:
            return dict(zip(self.column_names, row))
        return None

    @asyncio.coroutine
    def fetchone(self):
        """Returns next row of a query result set
        """
        row = yield from self._fetch_row()
        if row:
            return self._row_to_python(row, self.description)
        return None

    @asyncio.coroutine
    def fetchall(self):
        """Returns all rows of a query result set
        """
        if not self._have_unread_result():
            raise errors.InterfaceError(_ERR_NO_RESULT_TO_FETCH)
        (rows, eof) = yield from self._connection.get_rows()
        if self._nextrow[0]:
            rows.insert(0, self._nextrow[0])
        res = [self._row_to_python(row, self.description)
               for row in rows]
        yield from self._handle_eof(eof)
        rowcount = len(rows)
        if rowcount >= 0 and self._rowcount == -1:
            self._rowcount = 0
        self._rowcount += rowcount
        return res


class AioMySQLCursorNamedTuple(AioMySQLCursor):
    """
    Cursor fetching rows as named tuple.

    The fetch methods of this class will return namedtuples instead of tuples.
    Each row is returned as a namedtuple and the values can be accessed as:
    row.col1, row.col2
    """
    def _row_to_python(self, rowdata, desc=None):
        """Convert a MySQL text result row to Python types

        Returns a named tuple.
        """
        row = self._connection.converter.row_to_python(rowdata, desc)
        if row:
            # pylint: disable=W0201
            self.named_tuple = namedtuple('Row', self.column_names)
            # pylint: enable=W0201
            return self.named_tuple(*row)

    @asyncio.coroutine
    def fetchone(self):
        """Returns next row of a query result set
        """
        row = yield from self._fetch_row()
        if row:
            return self._row_to_python(row, self.description)
        return None

    @asyncio.coroutine
    def fetchall(self):
        """Returns all rows of a query result set
        """
        if not self._have_unread_result():
            raise errors.InterfaceError(_ERR_NO_RESULT_TO_FETCH)
        (rows, eof) = yield from self._connection.get_rows()
        if self._nextrow[0]:
            rows.insert(0, self._nextrow[0])
        res = [self._row_to_python(row, self.description)
               for row in rows]
        yield from self._handle_eof(eof)
        rowcount = len(rows)
        if rowcount >= 0 and self._rowcount == -1:
            self._rowcount = 0
        self._rowcount += rowcount
        return res


class AioMySQLCursorBufferedDict(AioMySQLCursorDict, AioMySQLCursorBuffered):
    """
    Buffered Cursor fetching rows as dictionaries.
    """
    @asyncio.coroutine
    def fetchone(self):
        """Returns next row of a query result set
        """
        row = yield from self._fetch_row()
        if row:
            return self._row_to_python(row, self.description)
        return None

    @asyncio.coroutine
    def fetchall(self):
        """Returns all rows of a query result set
        """
        if self._rows is None:
            raise errors.InterfaceError(_ERR_NO_RESULT_TO_FETCH)
        res = []
        for row in self._rows[self._next_row:]:
            res.append(self._row_to_python(
                row, self.description))
        self._next_row = len(self._rows)
        return res


class AioMySQLCursorBufferedNamedTuple(AioMySQLCursorNamedTuple, AioMySQLCursorBuffered):
    """
    Buffered Cursor fetching rows as named tuple.
    """
    @asyncio.coroutine
    def fetchone(self):
        """Returns next row of a query result set
        """
        row = yield from self._fetch_row()
        if row:
            return self._row_to_python(row, self.description)
        return None

    @asyncio.coroutine
    def fetchall(self):
        """Returns all rows of a query result set
        """
        if self._rows is None:
            raise errors.InterfaceError(_ERR_NO_RESULT_TO_FETCH)
        res = []
        for row in self._rows[self._next_row:]:
            res.append(self._row_to_python(
                row, self.description))
        self._next_row = len(self._rows)
        return res
