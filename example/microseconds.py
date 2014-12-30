

"""
Example using MySQL Connector/Python showing:
* How to save timestamps including microseconds
* Check the MySQL server version
NOTE: This only works with MySQL 5.6.4 or greater. This example will work
with earlier versions, but the microseconds information will be lost.
Story: We keep track of swimmers in a freestyle 4x 100m relay swimming event
with millisecond precision.
"""

from datetime import time
import asyncio
import mysql_async.connector

CREATE_TABLE = (
    "CREATE TABLE relay_laps ("
    "teamid TINYINT UNSIGNED NOT NULL, "
    "swimmer TINYINT UNSIGNED NOT NULL, "
    "lap TIME(3), "
    "PRIMARY KEY (teamid, swimmer)"
    ") ENGINE=InnoDB"
)

def main(config):
    output = []
    cnx = mysql_async.connector.Connect(**config)
    yield from cnx.connect()
    if cnx.get_server_version() < (5,6,4):
        output.append(
            "MySQL {0} does not support fractional precision"\
            " for timestamps.".format(cnx.get_server_info()))
        return output
    cursor = yield from cnx.cursor()

    try:
        yield from cursor.execute("DROP TABLE IF EXISTS relay_laps")
    except:
        # Ignoring the fact that it was not there
        pass
    yield from cursor.execute(CREATE_TABLE)

    teams = {}
    teams[1] = [
        (1, time(second=47, microsecond=510000)),
        (2, time(second=47, microsecond=20000)),
        (3, time(second=47, microsecond=650000)),
        (4, time(second=46, microsecond=60000)),
    ]

    insert = "INSERT INTO relay_laps (teamid,swimmer,lap) VALUES (%s,%s,%s)"
    for team, swimmers in teams.items():
        for swimmer in swimmers:
            yield from cursor.execute(insert, (team, swimmer[0], swimmer[1]))
    #cnx.commit()

    yield from cursor.execute("SELECT * FROM relay_laps")
    for row in (yield from cursor.fetchall()):
        output.append("{0: 2d} | {1: 2d} | {2}".format(*row))

    try:
        yield from cursor.execute("DROP TABLE IF EXISTS relay_laps")
    except:
        # Ignoring the fact that it was not there
        pass

    cursor.close()
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
