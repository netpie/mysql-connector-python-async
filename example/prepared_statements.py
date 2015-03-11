
"""Example using MySQL Prepared Statements
Example using MySQL Connector/Python showing:
* usage of Prepared Statements
"""
import asyncio
import mysql_async.connector
from mysql_async.connector.cursor import AioMySQLCursorPrepared


def main(config):
    output = []
    cnx = mysql_async.connector.Connect(**config)
    yield from cnx.connect()

    curprep =yield from cnx.cursor(cursor_class=AioMySQLCursorPrepared)
    cur = yield from cnx.cursor()

    # Drop table if exists, and create it new
    stmt_drop = "DROP TABLE IF EXISTS names"
    yield from cur.execute(stmt_drop)

    stmt_create = (
        "CREATE TABLE names ("
        "id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT, "
        "name VARCHAR(30) DEFAULT '' NOT NULL, "
        "cnt TINYINT UNSIGNED DEFAULT 0, "
        "PRIMARY KEY (id))"
        )
    yield from cur.execute(stmt_create)

    # Connector/Python also allows ? as placeholders for MySQL Prepared
    # statements.
    prepstmt = "INSERT INTO names (name) VALUES (%s)"

    # Preparing the statement is done only once. It can be done before
    # without data, or later with data.
    yield from curprep.execute(prepstmt)

    # Insert 3 records
    names = ('Geert', 'Jan', 'Michel')
    for name in names:
        yield from curprep.execute(prepstmt, (name,))
        yield from cnx.commit()

    # We use a normal cursor issue a SELECT
    output.append("Inserted data")
    yield from cur.execute("SELECT id, name FROM names")
    for row in (yield from cur.fetchall()):
        output.append("{0} | {1}".format(*row))
    # Cleaning up, dropping the table again
    yield from cur.execute(stmt_drop)
    yield from curprep.close()
    cnx.close()
    print(output)

if __name__ == '__main__':

    config = {
        'unix_socket': '/var/run/mysqld/mysqld.sock',
        'database': 'test',
        'user': 'user1',
        'password': 'user1',
        'charset': 'utf8',
        'use_unicode': True,
        'get_warnings': True,
    }

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(config))