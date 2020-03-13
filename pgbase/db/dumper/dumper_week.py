import os
import datetime
import gzip
import getpass

schemas = ['farma', 'geocoder',  'lancet', 'logs', 'metadata', 'novartis', 'rgs', 'rosreestr_state', 'rr_data']

now = datetime.datetime.now()

for schema in schemas:

    dir = 'dump_database/%s' % (schema + '_' + str(now.year) + '_' + str(now.month) + '_' + str(now.day))
    os.system('mkdir -p %s' % dir)
    print(schema, ' dump started')
    command = 'pg_dump mldata --username=jazz -w -Fd -n %s -f %s' % (schema, dir)
    os.system(command)
    print(schema, ' is dumped to %s' % dir)

