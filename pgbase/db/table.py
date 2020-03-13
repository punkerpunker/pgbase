import pandas as pd
import os
import time
from io import StringIO
from mlbase.db.engine import MLData
from mlbase.db.indexer import index_tables


class Table:
    def __init__(self, schema_table, db=None):
        if not db:
            self.db = MLData()
        else:
            self.db = db
        self.engine = self.db.engine
        self.cur = self.db.cur
        self.schema_table = schema_table
        self.schema_name = schema_table.split('.')[0]
        self.table_name = schema_table.split('.')[1]
        super(Table, self).__init__()
        self.columns = self.get_column_types()

    def copy_df(self, df, if_exists='fail'):
        output = 'temp_copy_upload_' + str(int(time.time())) + '.csv'
        copy_sql = """COPY \"%s\".\"%s\" ("%s") FROM stdin WITH CSV HEADER DELIMITER as '\t' QUOTE '"' """ % \
                   (self.schema_name, self.table_name, '","'.join(df.columns))
        cur = self.db.cur
        df.head(0).to_sql(self.table_name, self.db.engine, schema=self.schema_name, index=False, if_exists=if_exists)
        df.to_csv(output, sep='\t', index=False, escapechar='"')
        with open(output, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
        os.remove(output)
        self.db.commit()

    def get_column_types(self):
        query = "select column_name, data_type " \
                " from information_schema.columns " \
                " where table_schema = '"+self.schema_name+"'"\
                " and table_name = '"+self.table_name+"'"
        df = pd.read_sql(query, self.engine)
        types = pd.Series(df.data_type.values, index=df.column_name).to_dict()
        return types

    def drop_column(self, column_name):
        query = 'alter table "'+self.schema_name+'"."'+self.table_name+'" drop column if exists "'+column_name+'"'
        self.cur.execute(query)
        self.db.commit()

    def check_column_existance(self, column_name):
        self.cur.execute(" SELECT '"+column_name+"' FROM information_schema.columns " 
                         " WHERE table_schema = '"+self.schema_name+"' "
                         " and table_name='"+self.table_name+"' "
                         " and column_name='"+column_name+"'")
        check = self.cur.fetchall()
        return bool(len(check)*True)

    def get_column_type(self, column_name):
        self.cur.execute(" select data_type from information_schema.columns "
                         " where column_name = '"+column_name+"' "
                         " and table_schema = '"+self.schema_name+"' "
                         " and table_name = '"+self.table_name+"'")
        data_type = self.cur.fetchall()[0][0]
        return data_type

    def alter_column_type(self, column_name, new_type):
        self.cur.execute('alter table "'+self.schema_name+'"."'+self.table_name+'" '
                         ' alter column "'+column_name+'" type '+str(new_type)+' '
                         ' using "'+column_name+'"::'+str(new_type))

    def add_geometry_column(self,
                            lat_column, lng_column,
                            geometry_column_name='geometry', geo_type='POINT',
                            inplace=False):
        exists = self.check_column_existance(column_name=geometry_column_name)
        if not exists or inplace:
            if inplace:
                self.drop_column(column_name=geometry_column_name)
            self.cur.execute("SELECT AddGeometryColumn ('"+self.schema_name+"',"
                             "'"+self.table_name+"','"+geometry_column_name+"',4326,'"+geo_type+"',2, false)")
            self.db.commit()
            self.cur.execute("update \""+self.schema_name+"\".\""+self.table_name+"\" "
                             "set "+geometry_column_name+" = "
                             "ST_SetSRID(ST_MakePoint(\""+lng_column+"\", \""+lat_column+"\"),4326);")
            self.db.commit()
        else:
            raise Exception(geometry_column_name + ' already exists '
                                                   ' in table ' + self.schema_name + '.' + self.table_name + ' '
                                                   'specify inplace=True')

    def drop(self, cascade=False):
        if cascade is False:
            self.cur.execute('drop table if exists "'+self.schema_name+'"."'+self.table_name+'"')
            self.db.commit()
        else:
            self.cur.execute('drop table if exists "' + self.schema_name + '"."' + self.table_name + '" cascade')
            self.db.commit()

    def deduplicate(self):
        self.cur.execute(
            'select * into '+self.schema_name+'."'+self.table_name+'dedups" from (select * '
            'from '+self.schema_name+'."'+self.table_name+'" union  '
            'select * from '+self.schema_name+'."'+self.table_name+''
            '") as foo;')
        self.db.commit()
        self.cur.execute('drop table '+self.schema_name+'."'+self.table_name+'" cascade;')
        self.db.commit()
        self.cur.execute('alter table '+self.schema_name+'."'+self.table_name+'dedups" '
                                                                              'rename to "'+self.table_name+'";')
        self.db.commit()

    def rename(self, new_name):
        self.cur.execute(
            '''alter table if exists "{}"."{}" rename to "{}"'''.format(self.schema_name, self.table_name, new_name))
        self.db.commit()
        self.table_name = new_name
        self.schema_table = self.schema_name + '.' + new_name

    def add_index(self, column_name, index_name='default', index_type='btree'):
        if index_name is 'default':
            index_name = self.schema_name + '_' + self.table_name + '_' + column_name + '_index'
        jsn = {self.schema_name:{self.table_name:{column_name:{'name':index_name, 'type':index_type}}}}
        index_tables(jsn, self.db)

    def rename_coulmns(self, dict_):
        for key in dict_.keys():
            sql_query = 'ALTER TABLE {0} RENAME {1} TO {2}'.format(self.schema_table, key, dict_[key])
            self.db.cur.execute(sql_query)
        self.db.commit()
