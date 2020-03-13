import pandas as pd
from collections import OrderedDict
from pgbase.db.table import Table


class Query:
    def __init__(self, table_schema, db=None):
        self.data_query = OrderedDict([('select', None),
                                       ('from', None),
                                       ('joins', None),
                                       ('where', None),
                                       ('group by', None)])
        self.table = Table(table_schema, db)
        self.from_()

    def cut_rivers(self, schema_name='metadata', table_name='no_rivers'):
        if 'city_id' not in self.table.columns or 'square_id' not in self.table.columns:
            raise Exception('Need columns city_id, square_id!')
        else:
            self.join(keys_dict={schema_name+'.'+table_name:{'square_id': 'square_id_join','city_id': 'city_id_join'}},
                      type='inner')

    def select(self, columns=None):
        query = ''
        columns = self.list_comprehension_handler(columns)
        if columns:
            for column in columns:
                column = self.brackets_point_handler(column)
                query += column + ','
        else:
            query += ' * '
        self.data_query['select'] = query

    def add_city_id(self, lat, lng, select=True, inplace=False):
        if select:
            self.data_query['select'] += ' city_id, town,'
        query = ' left join metadata.cities_grids ' \
                ' on '+str(self.point_handler(lat))+' < latmax and '+str(self.point_handler(lat))+' > latmin ' \
                ' and '+str(self.point_handler(lng))+' < longmax and '+str(self.point_handler(lng))+' > longmin '
        self.query_assign_handler(query, inplace, 'joins')

    def from_(self, table_schema=None):
        if not table_schema:
            query = self.point_handler(self.table.schema_table)
        else:
            schema_name = table_schema.split('.')[0]
            table_name = table_schema.split('.')[1]
            query = self.point_handler(schema_name+'.'+table_name)
        query = self.end_query_replacements(query)
        self.query_assign_handler(query, False, 'from')

    def join(self, keys_dict, type ='left', inplace=False):
        query = ''
        for new_table, key_pairs in keys_dict.items():
            query += type + ' join '
            query += ' ' + self.point_handler(new_table) + ' on '
            for key_left, key_right in key_pairs.items():
                query += ' ' + self.point_handler(key_left) + ' = ' + self.point_handler(key_right) + ' and'
            query = self.end_query_replacements(query)
        self.query_assign_handler(query, inplace, 'joins')

    def groupby(self, column, inplace=False):
        columns = self.list_comprehension_handler(column)
        query = ' '
        for col in columns:
            query += ' ' + self.point_handler(col) + ''
        query = self.end_query_replacements(query)
        self.query_assign_handler(query, inplace, 'group by')

    def like_filter(self, filters, inplace=False):
        query = ''
        for key, value in filters.items():
            if key.startswith('~'):
                query += ' ' + str(self.point_handler(key[1:])) + ' not like '
            else:
                query += ' ' + str(self.point_handler(key)) + ' like '
            query += "'%%"+value+"%%'"
            query += " and"
        self.query_assign_handler(query, inplace, 'where')

    def in_filter(self, filters, inplace=False):
        query = ''
        for key, values in filters.items():
            if key.startswith('~'):
                query += ' ' + str(self.point_handler(key[1:])) + ' not in ('
            else:
                query += ' ' + str(self.point_handler(key)) + ' in ('
            values = self.list_comprehension_handler(values)
            for value in values:
                if isinstance(value, str):
                    query += "'"+value+"',"
                else:
                    query += "" + str(value) + ","
            query = self.end_query_replacements(query)
            query += ") and"
        self.query_assign_handler(query, inplace, 'where')

    def distance_filter(self, point, column_geom, distance, table_name=None, select=True, inplace=False):
        if not table_name:
            table_name = self.table.table_name
        query = ' ST_Intersects(ST_Buffer(ST_GeomFromText(\''\
                'POINT('+str(point[1])+' '+str(point[0])+')\', 4326)::geography, ' \
                ''+str(distance)+')::geometry, '+table_name+'.'+column_geom+') and'
        self.query_assign_handler(query, inplace, 'where')
        if select:
            query = ' ST_Distance(ST_GeomFromText(\'' \
                    ' POINT(' + str(point[1]) + ' ' + str(point[0]) + ')\', 4326)::geography, ' \
                    ' '+table_name+'.'+column_geom+'::geography, False) as distance,'
            if query not in self.data_query['select']:
                self.data_query['select'] += query

    def generate_sql(self):
        import re
        sql = ''
        for block, query in self.data_query.items():
            if query:
                if block == 'joins':
                    sql += self.end_query_replacements(query) + ' '
                else:
                    sql += block + ' ' + self.end_query_replacements(query) + ' '
        return sql

    def get(self):
        sql = self.generate_sql()
        df = pd.read_sql(sql, self.table.engine)
        return df

    def query_assign_handler(self, query, inplace, block):
        if inplace or not self.data_query[block]:
            self.data_query[block] = query
        else:
            self.data_query[block] += query

    def existance_handler(self, columns):
        not_columns = []
        for column in columns:
            if column not in self.table.columns:
                not_columns.append(column)
        if len(not_columns) == 0:
            pass
        else:
            raise Exception('Columns '+str(not_columns)+' are not in table.')

    @staticmethod
    def end_query_replacements(query):
        if query.endswith('and'):
            query = query[:-3]
        if query.endswith(','):
            query = query[:-1]
        return query

    @staticmethod
    def brackets_point_handler(string):
        corrected = ''
        if 'as' in string:
            as_ = ' as'+string.split('as')[1]
            string = string.split(' as ')[0]
        else:
            as_ = ''
        agg_functions = ['sum','avg','median','count']
        num_brackets = string.count('(')
        if num_brackets == 0:
            corrected += Query.point_handler(string)
        if num_brackets == 1:
            agg = string.split('(')[0]
            column = string.split('(')[1].split(')')[0]
            corrected += agg+'('+Query.point_handler(column)+')'
        else:
            for x in range(num_brackets):
                if x == 0:
                    agg = string.replace(' ','').split('(')[x]
                else:
                    agg_str = string.split('(')[x]
                    for ag_f in agg_functions:
                        if agg_str.endswith(ag_f):
                            agg = ag_f
                if x != num_brackets-1:
                    action = string.replace(' ','').split(')')[x+1][0]
                else:
                    action = ''
                column = string.split('(')[x + 1].split(')')[0]
                corrected += agg+'('+Query.point_handler(column)+')'+action
        corrected += as_
        corrected = corrected.replace('""','"')
        return corrected

    @staticmethod
    def point_handler(string):
        if '.' in string:
            string = '"' + string.split('.')[0] + '"."' + string.split('.')[1] + '"'
        return string

    @staticmethod
    def list_comprehension_handler(to_test):
        if isinstance(to_test, str) or isinstance(to_test, float) or isinstance(to_test, int):
            _list = []
            _list.append(to_test)
            return _list
        elif isinstance(to_test, tuple):
            _list = list(to_test)
            return _list
        else:
            return to_test

