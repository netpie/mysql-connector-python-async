

"""
Example using MySQL Connector/Python showing:
* using warnings
"""
import asyncio
import mysql_async.connector

STMT = "SELECT 'abc'+1"


def main(config):
    output = []
    config['get_warnings'] = True
    db = mysql_async.connector.Connect(**config)
    yield from db.connect()
    cursor = yield from db.cursor()
    db.sql_mode = ''

    output.append("Executing '%s'" % STMT)
    yield from cursor.execute(STMT)
    yield from cursor.fetchall()

    warnings = cursor.fetchwarnings()
    if warnings:
        for w in warnings:
            output.append("%d: %s" % (w[1],w[2]))
    else:
        output.append("We should have got warnings.")
        #raise Exception("Got no warnings")

    cursor.close()
    db.close()
    print(output)
    return output


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