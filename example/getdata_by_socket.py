import asyncio
import mysql_async.connector


"""
Example using MySQL Connector/Python showing:
* that show engines works..
"""

import sys, os


@asyncio.coroutine
def main(config):
    output = []
    db = mysql_async.connector.Connect(**config)
    yield from db.connect()

    cursor = yield from db.cursor()

    # Select it again and show it
    stmt_select = "SHOW ENGINES"
    yield from cursor.execute(stmt_select)
    rows = yield from cursor.fetchall()

    for row in rows:
        output.append(repr(row))

    db.close()
    print("current database:", (yield from db.database))
    print('\n'.join(output))

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