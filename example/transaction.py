

"""
Example using MySQL Connector/Python showing:
* dropping and creating a table
* using warnings
* doing a transaction, rolling it back and committing one.
"""
import asyncio
import mysql_async.connector


def main(config):
    output = []
    db = mysql_async.connector.Connect(**config)
    yield from db.connect()
    cursor = yield from db.cursor()

    # Drop table if exists, and create it new
    stmt_drop = "DROP TABLE IF EXISTS names"
    yield from cursor.execute(stmt_drop)

    stmt_create = """
    CREATE TABLE names (
        id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
        name VARCHAR(30) DEFAULT '' NOT NULL,
        cnt TINYINT UNSIGNED DEFAULT 0,
        PRIMARY KEY (id)
    ) ENGINE=InnoDB"""
    yield from cursor.execute(stmt_create)

    warnings = cursor.fetchwarnings()
    if warnings:
        ids = [ i for l,i,m in warnings]
        output.append("Oh oh.. we got warnings..")
        if 1266 in ids:
            output.append("""
            Table was created as MYISAM, no transaction support.

            Bailing out, no use to continue. Make sure InnoDB is available!
            """)
            db.close()
            print(output)
            return

    # Insert 3 records
    yield from db.start_transaction()
    output.append("Inserting data")
    names = ( ('Geert',), ('Jan',), ('Michel',) )
    stmt_insert = "INSERT INTO names (name) VALUES (%s)"
    yield from cursor.executemany(stmt_insert, names)

    # Roll back!!!!
    output.append("Rolling back transaction")
    yield from db.rollback()

    # There should be no data!
    stmt_select = "SELECT id, name FROM names ORDER BY id"
    yield from cursor.execute(stmt_select)
    rows = None
    try:
        rows = yield from cursor.fetchall()
    except mysql_async.connector.InterfaceError as e:
        raise

    if rows == []:
        output.append("No data, all is fine.")
    else:
        output.append("Something is wrong, we have data although we rolled back!")
        output.append(rows)
        cursor.close()
        db.close()
        print(output)
        return output

    # Do the insert again.
    yield from cursor.executemany(stmt_insert, names)

    # Data should be already there
    yield from cursor.execute(stmt_select)
    output.append("Data before commit:")
    for row in (yield from cursor.fetchall()):
        output.append("%d | %s" % (row[0], row[1]))

    # Do a commit
    yield from db.commit()

    yield from cursor.execute(stmt_select)
    output.append("Data after commit:")
    for row in (yield from cursor.fetchall()):
        output.append("%d | %s" % (row[0], row[1]))

    # Cleaning up, dropping the table again
    yield from cursor.execute(stmt_drop)

    cursor.close()
    db.close()
    for i in output:
        print(i)


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
