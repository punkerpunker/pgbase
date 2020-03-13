from pgbase.db.engine import DB
import pandas as pd
import psycopg2
import getpass


def index_tables(jsn, db):

    # - jsn format must be {schema_name : { table_name : { column_name : { index_name, type }}}}

    cur = db.cur

    for schema_name in jsn:
        for table_name in jsn[schema_name]:
            for column_name in jsn[schema_name][table_name]:

                index_name = jsn[schema_name][table_name][column_name]['name']
                index_type = jsn[schema_name][table_name][column_name]['type']

                print('about to add %s to %s.%s' % (index_name, schema_name, table_name))

                sql_query = 'create index %s ' \
                            'on %s.%s using %s(%s)' % (index_name, schema_name, table_name, index_type, column_name)

                cur.execute(sql_query)

                db.commit()

                print('index %s created in %s.%s' % (index_name, schema_name, table_name))


def create_jsn(schemas, db):

    # - schemas format - ['schema_name_1', 'schema_name_2', ...]

    engine = db.engine

    jsn = {}

    for schema_name in schemas:

        jsn[schema_name] = {}

        sql_query = 'select table_name ' \
                    'from information_schema.tables ' \
                    'where table_schema = \'%s\'' % schema_name
        df = pd.read_sql_query(sql_query, engine)
        tables = df['table_name'].tolist()

        for table_name in tables:

            jsn[schema_name][table_name] = {}

            standard_column_names(jsn, schema_name, table_name, db)

    return jsn


def standard_column_names(jsn, schema_name, table_name, db):

    # - jsn format { schema_name : { table_name : {}}}
    # - add to jsn common column names

    engine = db.engine

    sql_query = 'select column_name ' \
                'from information_schema.columns ' \
                'where table_schema = \'%s\' and table_name = \'%s\' ' % (schema_name, table_name)
    df = pd.read_sql_query(sql_query, engine)
    columns = df['column_name'].tolist()

    if 'id' in columns:
        jsn[schema_name][table_name]['id'] = {}
        jsn[schema_name][table_name]['id']['name'] = 'id_index' + '_' + schema_name + '_' + table_name
        jsn[schema_name][table_name]['id']['type'] = 'btree'

    if 'name' in columns:
        jsn[schema_name][table_name]['name'] = {}
        jsn[schema_name][table_name]['name']['name'] = 'name_index' + '_' + schema_name + '_' + table_name
        jsn[schema_name][table_name]['name']['type'] = 'btree'

    if 'address' in columns:
        jsn[schema_name][table_name]['address'] = {}
        jsn[schema_name][table_name]['address']['name'] = 'address_index' + '_' + schema_name + '_' + table_name
        jsn[schema_name][table_name]['address']['type'] = 'btree'

    if 'gc_address' in columns:
        jsn[schema_name][table_name]['gc_address'] = {}
        jsn[schema_name][table_name]['gc_address']['name'] = 'gc_address_index' + '_' + schema_name + '_' + table_name
        jsn[schema_name][table_name]['gc_address']['type'] = 'btree'

    if 'date' in columns:
        jsn[schema_name][table_name]['date'] = {}
        jsn[schema_name][table_name]['date']['name'] = 'date_index' + '_' + schema_name + '_' + table_name
        jsn[schema_name][table_name]['date']['type'] = 'btree'

    if 'date_added' in columns:
        jsn[schema_name][table_name]['date_added'] = {}
        jsn[schema_name][table_name]['date_added']['name'] = 'date_added_index' + '_' + schema_name + '_' + table_name
        jsn[schema_name][table_name]['date_added']['type'] = 'btree'

    return jsn


def get_jsn_yandex_organization_rubric(db):

    engine = db.engine

    jsn = {'yandex_objects': {}}

    sql_query = 'select table_name ' \
                'from information_schema.tables ' \
                'where table_schema = \'yandex_objects\' ' \
                '   and table_name like \'organization_2%%\' '

    df = pd.read_sql_query(sql_query, engine)
    tables = df['table_name'].tolist()

    for table_name in tables:
        jsn['yandex_objects'][table_name] = {}

        jsn['yandex_objects'][table_name]['id'] = {}
        jsn['yandex_objects'][table_name]['id']['name'] = table_name + '_id_index'
        jsn['yandex_objects'][table_name]['id']['type'] = 'btree'

        jsn['yandex_objects'][table_name]['gc_address'] = {}
        jsn['yandex_objects'][table_name]['gc_address']['name'] = table_name + '_gc_address_index'
        jsn['yandex_objects'][table_name]['gc_address']['type'] = 'btree'

    sql_query = 'select table_name  ' \
                'from information_schema.tables ' \
                'where table_schema = \'yandex_objects\' ' \
                '   and table_name like \'%%organization_rubric_2%%\' ' \
                '   and table_name not like \'%%excess\''

    df = pd.read_sql_query(sql_query, engine)
    tables = df['table_name'].tolist()

    for table_name in tables:
        jsn['yandex_objects'][table_name] = {}

        jsn['yandex_objects'][table_name]['organization_id'] = {}
        jsn['yandex_objects'][table_name]['organization_id']['name'] = table_name + '_organization_id_index'
        jsn['yandex_objects'][table_name]['organization_id']['type'] = 'btree'

        jsn['yandex_objects'][table_name]['rubric_id'] = {}
        jsn['yandex_objects'][table_name]['rubric_id']['name'] = table_name + '_rubric_id_index'
        jsn['yandex_objects'][table_name]['rubric_id']['type'] = 'btree'

    sql_query = 'select table_name ' \
                'from information_schema.tables ' \
                'where table_schema = \'yandex_objects\' ' \
                '   and table_name like \'rubric_2%%\' '

    df = pd.read_sql_query(sql_query, engine)
    tables = df['table_name'].tolist()

    for table_name in tables:
        jsn['yandex_objects'][table_name] = {}

        jsn['yandex_objects'][table_name]['id'] = {}
        jsn['yandex_objects'][table_name]['id']['name'] = table_name + '_id_index'
        jsn['yandex_objects'][table_name]['id']['type'] = 'btree'

        jsn['yandex_objects'][table_name]['name'] = {}
        jsn['yandex_objects'][table_name]['name']['name'] = table_name + '_gc_address_index'
        jsn['yandex_objects'][table_name]['name']['type'] = 'btree'

    return jsn








