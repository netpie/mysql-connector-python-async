"""
Example using MySQL Connector/Python showing:
* How to get datetime, date and time types
* Shows also invalid dates returned and handled
* Force sql_mode to be not set for the active session
"""

from datetime import datetime
import asyncio
import mysql_async.connector
import mysql.connector.errors

# Note that by default MySQL takes invalid timestamps. This is for
# backward compatibility. As of 5.0, use sql modes NO_ZERO_IN_DATE,NO_ZERO_DATE
# to prevent this.
_adate = datetime(1977, 6, 14, 21, 10, 00)
DATA = [
    (_adate.date(), _adate, _adate.time()),
    ('0000-00-00', '0000-00-00 00:00:00', '00:00:00'),
    ('1000-00-00', '9999-00-00 00:00:00', '00:00:00'),
]


@asyncio.coroutine
def main(config):
    output = []
    db = mysql_async.connector.Connect(**config)
    yield from db.connect()
    cursor = yield from db.cursor()

    tbl = 'myconnpy_dates'

    yield from cursor.execute('SET sql_mode = ""')

    # Drop table if exists, and create it new
    stmt_drop = "DROP TABLE IF EXISTS {0}".format(tbl)
    yield from cursor.execute(stmt_drop)

    stmt_create = (
        "CREATE TABLE {0} ( "
        "  `id` tinyint(4) NOT NULL AUTO_INCREMENT, "
        "  `c1` date DEFAULT NULL, "
        "  `c2` datetime NOT NULL, "
        "  `c3` time DEFAULT NULL, "
        "  `changed` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP "
        "    ON UPDATE CURRENT_TIMESTAMP, "
        "PRIMARY KEY (`id`))"
    ).format(tbl)
    yield from cursor.execute(stmt_create)

    # not using executemany to handle errors better
    stmt_insert = ("INSERT INTO {0} (c1,c2,c3) VALUES "
                   "(%s,%s,%s)".format(tbl))
    for data in DATA:
        try:
            yield from cursor.execute(stmt_insert, data)
        except (mysql.connector.errors.Error, TypeError) as exc:
            output.append("Failed inserting {0}\nError: {1}\n".format(
                data, exc))
            raise

    # Read the names again and print them
    stmt_select = "SELECT * FROM {0} ORDER BY id".format(tbl)
    yield from cursor.execute(stmt_select)

    for row in (yield from cursor.fetchall()):
        output.append("%3s | %10s | %19s | %8s |" % (
            row[0],
            row[1],
            row[2],
            row[3],
        ))

    # Cleaning up, dropping the table again
    yield from cursor.execute(stmt_drop)

    yield from cursor.close()
    db.close()
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
