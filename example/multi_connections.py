import asyncio
import mysql_async.connector
import mysql_async.connector.pooling

CREATE_TABLE = (
   "CREATE TABLE names ("
        "    id INT UNSIGNED NOT NULL AUTO_INCREMENT, "
        "    name NVARCHAR(30) DEFAULT '' NOT NULL, "
        "    info TEXT ,"
        "    age TINYINT UNSIGNED DEFAULT '30', "
        "    PRIMARY KEY (id))"
)


@asyncio.coroutine
def main(pcnx, loop):
    cnx = yield from pcnx.get_connection()  # fetch a Mysqlconnector from a pool
    cursor = yield from cnx.cursor()
    # Drop table if exists, and create it new
    stmt_drop = "DROP TABLE IF EXISTS names"
    yield from cursor.execute(stmt_drop)
    yield from cursor.execute(CREATE_TABLE)

    print("Inserting data")
    names = (('Geert', 20,), ('Jan', 25,),('Geert', 20,), ('Jan', 25,),
             ('Geert', 20,), ('Geert', 20,), ('Jan', 25,), ('Jan', 25,),
             ('Geert', 20,), ('Geert', 20,), ('Jan', 25,), ('Jan', 25,),
             ('Geert', 20,), ('Geert', 20,), ('Jan', 25,), ('Jan', 25,),
             ('Geert', 20,), ('Jan', 25,), ('Jan', 25,), ('Michel', 18,))
    stmt_insert = "INSERT INTO names (name, age) VALUES (%s,%s)"
    yield from cursor.executemany(stmt_insert, names)
    print("Inserted {0} row{1}".format(cursor.rowcount, 's' if cursor.rowcount > 1 else ''))
    yield from asyncio.wait([foo1(pcnx), foo2(pcnx)], loop=loop)
    cursor.close()
    cnx.close()

@asyncio.coroutine
def foo2(pcnx):
    cnx = yield from pcnx.get_connection()
    cursor = yield from cnx.cursor()
    stmt_fetch = "select * from names limit 1,16 "  # changed last number to see which completed first.
    yield from cursor.execute(stmt_fetch)
    for row in (yield from cursor.fetchall()):
        pass
        #print("foo2-> ", row[0], row[1], row[2])
    print('foo2 completed!')
    cursor.close()
    cnx.close()

@asyncio.coroutine
def foo1(pcnx):
    cnx = yield from pcnx.get_connection()
    cursor = yield from cnx.cursor()
    stmt_fetch = "select * from names limit 1,12"  # changed last number to see which completed first.
    yield from cursor.execute(stmt_fetch)
    for row in (yield from cursor.fetchall()):
        #print("foo1-> ", row[0], row[1], row[2])
        pass
    print('foo1 completed!')

    cursor.close()
    cnx.close()

if __name__ == '__main__':

    config = {
        'unix_socket': '/var/run/mysqld/mysqld.sock',
        'database': 'test',
        'user': 'user1',
        'password': 'user1',
        'charset': 'utf8',
        'use_unicode': True,
        'get_warnings': True,
        'pool_size': 10,
    }
    #make a MySQLConnectionPool
    pcnx = mysql_async.connector.pooling.MySQLConPool(**config)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(pcnx, loop))