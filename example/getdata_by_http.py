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
    print('\n'.join(output))

if __name__ == '__main__':

    config = {
        'host': '127.0.0.1',
        'port': '3306',
        'database': 'test',
        'user': 'user1',
        'password': 'user1',
        'charset': 'utf8',
        'use_unicode': True,
        'get_warnings': True,
    }
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(config))
