import csv
import psycopg2
import luigi
config = luigi.configuration.get_config()

def connect_to_db():
    port = 5432
    host = config.get("postgres", "host")
    database = config.get("postgres", "database")
    user = config.get("postgres", "user")
    password = config.get("postgres", "password")
    conn = psycopg2.connect(
        host=host, port=port,
        database=database, user=user,
        password=password
    )
    conn.set_client_encoding('utf-8')
    return conn

censorship_methods_columns = (
    ('id', 'INTEGER'),
    ('name', 'TEXT'),
    ('description', 'TEXT')
)

country_censorship_methods_columns = (
    ('id', 'INTEGER PRIMARY KEY'),
    ('countryid', 'INTEGER'),
    ('censorshipmethodid', 'INTEGER')
)

def create_table(table, columns):
    coldefs = ','.join(
        '{name} {type}'.format(name=name, type=type) for name, type in columns
    )
    return "CREATE TABLE IF NOT EXISTS {table} ({coldefs})".format(table=table,
                                                                   coldefs=coldefs)

def insert_values(table, columns, vals):
    colnames = ','.join(name for name, _ in columns)
    values = "'"
    values += "','".join(vals)
    values += "'"
    query = """INSERT INTO {table} ({colnames}) VALUES ({values})
""".format(table=table, colnames=colnames, values=values)
    print query
    return query

def truncate_table(connection, table):
    connection.cursor().execute("TRUNCATE TABLE {table}".format(table=table))
    connection.commit()

def write_censorship_methods(connection):
    table = 'censorship_methods'
    cursor = connection.cursor()

    cursor.execute(create_table(table, censorship_methods_columns))
    connection.commit()
    truncate_table(connection, table)

    with open(config.get('ooni', 'censorship-methods-path')) as f:
        reader = csv.reader(f)
        for values in reader:
            values = map(lambda x: x.strip(), values)
            query = insert_values(table, censorship_methods_columns, values)
            cursor.execute(query)
    connection.commit()

def write_country_censorship_methods(connection):
    table = 'countrycensorship_method'
    cursor = connection.cursor()

    cursor.execute(create_table(table, country_censorship_methods_columns))
    connection.commit()
    truncate_table(connection, table)

    iso_alpha2_to_id = {}
    cursor.execute("SELECT iso_alpha2, id FROM country")
    for iso_alpha2, country_id in cursor:
        iso_alpha2_to_id[iso_alpha2] = country_id

    censorship_method_to_id = {}
    cursor.execute("SELECT name, id FROM censorship_methods")
    for name, method_id in cursor:
        censorship_method_to_id[name] = method_id

    with open(config.get('ooni', 'country-censorship-methods-path')) as f:
        reader = csv.reader(f)
        idx = 0
        for country_code, method_name in reader:
            values = [
                idx,
                iso_alpha2_to_id[country_code],
                censorship_method_to_id[method_name]
            ]
            values = map(str, values)
            query = insert_values(table, country_censorship_methods_columns, values)
            cursor.execute(query)
            idx += 1

    connection.commit()

def main():
    connection = connect_to_db()
    write_censorship_methods(connection)
    write_country_censorship_methods(connection)

if __name__ == "__main__":
    main()
