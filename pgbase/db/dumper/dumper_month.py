import os
import datetime
import gzip
import getpass
from systemd import journal

schemas = ['all-billboards', 'avtostat', 'banki', 'checkpoint', 'cian', 'clients', 'companies', 'double_gis', 'fias',
           'flats', 'gibdd', 'google', 'ifl', 'invitro', 'mediacatalog',  'next_gis_new',  'promoatlas', 'raw_data',
           'weather', 'yandex_objects', 'farma', 'geocoder',  'lancet', 'logs', 'metadata', 'novartis', 'rgs',
           'rosreestr_state', 'rr_data']

now = datetime.datetime.now()

for schema in schemas:

    dir = '/home/jazz/dump_database/%s' % (schema + '_' + str(now.year) + '_' + str(now.month) + '_' + str(now.day))
    os.system('mkdir -p %s' % dir)
    print(getpass.getuser())
    journal.send(schema + ' dump started')
    command = 'pg_dump mldata --username=jazz -w -Fd -n %s -f %s' % (schema, dir)
    os.system(command)
    print(schema, ' is dumped to %s' % dir)
    journal.send(schema + ' is dumped to %s' % dir)


