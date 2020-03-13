import sqlalchemy
import psycopg2
import getpass
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import table, column, select, update, insert
from sqlalchemy.exc import SAWarning
import getpass


class DB:
    def __init__(self, db="mldata", user=None, password=None, host="192.168.10.2"):
        if not user:
            user = getpass.getuser()
        if not password:
            self.password = getpass.getpass(prompt='Password for user '+user+': ', stream=None)
        else:
            self.password = password
        self.conn = psycopg2.connect(dbname=db, user=user, password=self.password, host=host, port=5432)
        self.engine = sqlalchemy.create_engine('postgresql+psycopg2://'+str(user)+':'
                                               ''+str(self.password)+'@' + host + '/' + db)
        self.cur = self.conn.cursor()

    def close(self):
        self.cur.close()
        self.conn.close()
        self.engine.dispose()

    def get_table(self, schema_name, table_name):
        metadata = sqlalchemy.MetaData(self.engine)
        return sqlalchemy.Table(table_name, metadata, schema=schema_name, autoload=True)

    def insert(self, schema_table, insert_dict):
        table = self.get_table(schema_table.split('.')[0], schema_table.split('.')[1])
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        sess = Session()
        ins = table.insert().values([insert_dict])
        sess.execute(ins)
        sess.commit()

    def list_tables_in_schema(self,schema):
        self.cur.execute('select distinct(table_name) from information_schema.tables where '
                         'table_schema = \'' + schema + '\' ;')
        list_of_table_names = self.cur.fetchall()
        return [x[0] for x in list_of_table_names]

    def vacuum(self, schema, table=None):
        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(0)
        if not table:
            list_of_schema_tables = self.schema_tables(schema)
            for name in list_of_schema_tables:
                print(schema, name)
                self.cur.execute('VACUUM (FULL) "' + schema + '"."' + name + '";')
                self.conn.commit()
        else:
            self.cur.execute('VACUUM (FULL) "' + schema + '"."' + table + '";')
        self.conn.set_isolation_level(old_isolation_level)

    def move_indexes(self, table_space_name='md0space', schema_name='punker', table_name='moved indexes'):
        df = pd.read_sql_query('select schemaname, tablename, indexname, tablespace from pg_indexes ;', self.engine)
        count_of_system_catalog = 0
        add_list = []
        list_of_table_space = []
        for index, row in df.iterrows():
            self.cur.execute(
                'select pg_relation_filepath(\'"' + str(row['schemaname']) + '"."' + str(row['indexname']) + '"\');')
            location = self.cur.fetchall()[0][0]
            add_list.append(location)
            tablespace = location.split('/')[0]
            list_of_table_space.append(tablespace)
            conn.commit()
            if tablespace != 'global' and tablespace != 'pg_tblspc' and ('pg_' not in row['schemaname']):
                alter_index = 'alter index "' + str(row['schemaname']) + \
                '"."' + str(row['indexname']) + '"set tablespace ' + table_space_name + ';'
                self.cur.execute(alter_index)
                self.conn.commit()
                count_of_system_catalog += 1
            else:
                pass
        df['location'] = add_list
        df.to_sql(table_name, con=self.engine, schema=schema_name, if_exists='append', index=False)

    def commit(self):
        self.conn.commit()

    def drop(self):
        self.cur.execute('drop table if exists '+self.schema_name+'."'+self.table_name+'"')
        self.commit()


class Loger:
    def __init__(self, schema_table, db):
        self.schema_table = schema_table
        self.db = db
        self.table_dict = {}

    def write_to_log(self, key, value):
        self.table_dict[key] = value

    def push(self):
        self.db.insert(self.schema_table, self.table_dict)
