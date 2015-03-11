"""
Example using MySQL Connector/Python showing:
* sending multiple statements and iterating over the results
"""
import asyncio
import mysql_async.connector


@asyncio.coroutine
def main(config):
    output = []
    db = mysql_async.connector.Connect(**config)
    yield from db.connect()
    cursor = yield from db.cursor()

    # Drop table if exists, and create it new
    stmt_drop = "DROP TABLE IF EXISTS names"
    yield from cursor.execute(stmt_drop)

    stmt_create = (
        "CREATE TABLE names ("
        "    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT, "
        "    name VARCHAR(30) DEFAULT '' NOT NULL, "
        "    info TEXT , "
        "    age TINYINT UNSIGNED DEFAULT '30', "
        "    PRIMARY KEY (id))"
    )
    yield from cursor.execute(stmt_create)

    info = "abc" * 10000

    stmts = [
        "INSERT INTO names (name) VALUES ('Geert')",
        "SELECT COUNT(*) AS cnt FROM names",
        "INSERT INTO names (name) VALUES ('Jan'),('Michel')",
        "SELECT name FROM names",
    ]

    # Note 'multi=True' when calling cursor.execute()
    result = yield from cursor.execute(' ; '.join(stmts), multi=True)
    while True:
        if result.with_rows:
            if result.statement == stmts[3]:
                output.append("Names in table: " +
                              ' '.join([name[0] for name in (yield from result.fetchall())]))
            else:
                rd = yield from result.fetchone()
                output.append(
                    "Number of rows: {0}".format(rd[0]))
        else:
            output.append("Inserted {0} row{1}".format(
                result.rowcount, 's' if result.rowcount > 1 else ''))
        if not (yield from result.next_exec_result()):
            break

    yield from cursor.execute(stmt_drop)

    cursor.close()
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