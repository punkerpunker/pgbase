import os
import datetime
import gzip
import getpass
import time
from systemd import journal


def main():

    while True:
        now = datetime.datetime.now()

        if now.day == 1:
            dumper_month()
            dumper_week()
        elif now.day == 8 or now.day == 15 or now.day == 22:
            dumper_week()
        wait_to_tomorrow()


def wait_to_tomorrow():

    tomorrow = datetime.datetime.replace(datetime.datetime.now() + datetime.timedelta(days=1), hour=1, minute=0, second=0)
    delta = tomorrow - datetime.datetime.now()
    time.sleep(delta.seconds)


def dumper_week():
    schemas = ['farma', 'geocoder', 'lancet', 'logs', 'metadata', 'novartis', 'rgs', 'rosreestr_state', 'rr_data']

    now = datetime.datetime.now()

    for schema in schemas:
        dir = 'dump_database/%s' % (schema + '_' + str(now.year) + '_' + str(now.month) + '_' + str(now.day))
        os.system('mkdir -p %s' % dir)
        journal.send(schema + ' dump started')
        command = 'pg_dump mldata --username=jazz -w -Fd -n %s -f %s' % (schema, dir)
        os.system(command)
        journal.send(schema + ' is dumped to %s' % dir)


def dumper_month():
    schemas = ['all-billboards', 'avtostat', 'banki', 'checkpoint', 'cian', 'clients', 'companies', 'double_gis',
               'fias', 'flats', 'gibdd', 'google', 'ifl', 'invitro', 'mediacatalog', 'next_gis_new', 'promoatlas',
               'raw_data', 'weather', 'yandex_objects']

    now = datetime.datetime.now()

    for schema in schemas:
        dir = 'dump_database/%s' % (schema + '_' + str(now.year) + '_' + str(now.month) + '_' + str(now.day))
        os.system('mkdir -p %s' % dir)
        journal.send(schema + ' dump started')
        command = 'pg_dump mldata --username=jazz -w -Fd -n %s -f %s' % (schema, dir)
        os.system(command)
        journal.send(schema + ' is dumped to %s' % dir)


main()