# Script for creating a partial dump of a MySQL database 
# Set table name and data retrieval conditions from it.
# Crawl-dumper collects, in the manner of a search engine, through foreign keys all related tables and data. 
# The result of the script's work will be a .sh file, which contains instructions for mysqldump. 
# Running it will create a dump of all necessary tables and their data.

# install python: apt-get install python
# install requirements: pip install sqlalchemy mysqlclient
# start: python ./dump.py

from sqlalchemy import create_engine, MetaData, Table, text, exc, inspect
from urllib.parse import urlparse

dsn = 'mysql://root:password@localhost/db_name' # database
backup_file = "snapshot.sql" # filename for dump
mysqldump_file = "mysqldump_snapshot.sh" # filename for mysqldump's instructions
default_start_table = "table_name" # table name, for example: album
default_start_condition = "id in (1,2,3,4,5)" # conditions after "where", example: dt_create > now() - interval 1 month
default_mysqldump_flags = "--no-tablespaces --no-create-info --replace --disable-keys"

#-------------------------------------------------------

engine = create_engine(dsn)
conn = engine.connect()
meta = MetaData()
meta.reflect(bind=engine)

start_table = input("table name, to start crawl (default: {}):\n".format(default_start_table)).strip()
where_condition = input("Crawl conditions, where (default: {}):\n".format(default_start_condition)).strip()
start_table = start_table if start_table else default_start_table
where_condition = where_condition if where_condition else default_start_condition

print("starting ...")
def db_query(query_str):
    global conn
    try:
        result = conn.execute(query_str)
    except exc.OperationalError as e:
        if 'client was disconnected' in str(e):
            conn = engine.connect()
            result = conn.execute(query_str)
        else:
            raise e
    return result

def dump_related_data(table_name, where_condition, checked_tables=None):
    result_set = {}
    table_key = table_name + '_' + where_condition
    if checked_tables is None:
        checked_tables = set()
    if table_key in checked_tables:
        return result_set

    checked_tables.add(table_key)

    tbl = Table(table_name, meta, autoload_with=engine)
    query_str = text(f"SELECT * FROM {tbl} WHERE {where_condition}")
    #print(query_str)
    result = db_query(query_str)
    rows = result.fetchall()
    if rows:
        if table_name not in result_set:
            result_set[table_name] = []
        for row in rows:
            result_set[table_name].append(row[0])

            for fkey in tbl.foreign_keys:
                related_table = fkey.column.table.name
                foreign_key = fkey.parent.name
                for row in rows:
                    foreign_key_index = tbl.columns.keys().index(str(foreign_key))
                    related_id = row[foreign_key_index]
                    if related_id is not None:
                        related_where = f"id = {related_id}"
                        related_result_set = dump_related_data(related_table, related_where, checked_tables=checked_tables)
                        for key, value in related_result_set.items():
                            if key in result_set:
                                result_set[key].extend(value)
                            else:
                                result_set[key] = value
    return result_set

def find_referencing_tables(start_table, meta):
    referencing_tables = {}
    inspector = inspect(engine)

    for table_name in inspector.get_table_names():
        tbl = Table(table_name, meta, autoload_with=engine)
        for fkey in tbl.foreign_keys:
            if fkey.column.table.name == start_table:
                referencing_tables[table_name] = fkey.parent.name

    return referencing_tables
def dump_referencing_data(start_table, meta, checked_tables=None):
    global where_condition

    result_set = {}
    if checked_tables is None:
        checked_tables = set()

    referencing_tables = find_referencing_tables(start_table, meta)
    for table_name, foreign_key in referencing_tables.items():
        tbl = Table(table_name, meta, autoload_with=engine)
        query_str = text(f"SELECT id FROM {table_name} WHERE {foreign_key} IN (SELECT id FROM {start_table} WHERE {where_condition})")
        result = db_query(query_str)
        rows = result.fetchall()
        if rows:
            if table_name not in result_set:
                result_set[table_name] = []
            for row in rows:
                result_set[table_name].append(row[0])
                related_where = f"id = {row[0]}"
                related_result_set = dump_related_data(table_name, related_where, checked_tables=None)
                for key, new_values in related_result_set.items():
                    if key in result_set:
                        existing_set = set(result_set[key])
                        updated_values = existing_set.union(set(new_values))
                        result_set[key] = list(updated_values)
                    else:
                        result_set[key] = new_values
    return result_set

print("collection outbound data...")
relations = dump_related_data(start_table, where_condition, checked_tables=None)
print("found tables: ", len(relations))
print("collection inbound data...")
referencing_relations = dump_referencing_data(start_table, meta, checked_tables=None)
print("found tables: ", len(referencing_relations))
for key, new_values in referencing_relations.items():
    if key in relations:
        existing_set = set(relations[key])
        updated_values = existing_set.union(set(new_values))
        relations[key] = list(updated_values)
    else:
        relations[key] = new_values

print("total tables to dump:", len(relations))

def generate_mysqldump_script(related_tables, script_filename):
    global dsn, backup_file, default_mysqldump_flags

    url = urlparse(dsn)
    username = url.username
    password = url.password
    hostname = url.hostname
    dbname = url.path[1:]
    with open(script_filename, 'w') as script_file:
        script_file.write("#!/bin/sh\n")
        script_file.write(f"echo '' > {backup_file}\n")
        for table, ids in related_tables.items():
            if ids:
                id_list = ','.join(map(str, ids))
                print(f"rows: {len(ids)},\ttable: {table}")
                command = f"mysqldump -u {username} -p{password} -h {hostname} {dbname} {table} {default_mysqldump_flags} --where=\"id IN ({id_list})\" >> {backup_file}\n"
                script_file.write(command)
        script_file.write("echo 'dump completed'")

generate_mysqldump_script(relations, mysqldump_file)
