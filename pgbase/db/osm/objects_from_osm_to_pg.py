import pandas as pd
import subprocess
import numpy as np
from sqlalchemy import create_engine
import sys
from database.db import MLData


def main():
    # - needs .osm.pbf file from http://download.geofabrik.de
    filename = '/home/marketinglogic/Skald/base_downloads/gb/great-britain-latest.osm.pbf'
    osm_to_pg(filename)


def osm_to_pg(filename):
    columns = ['amenity', 'shop', 'emergency', 'building', 'military', 'place', 'sport', 'leisure', 'tourism']
    engine = MLData().engine
    subprocess.check_call(['osmconvert '+filename+' '
                           '--all-to-nodes '
                           '--csv="@id @lon @lat amenity shop name emergency '
                           'building military place sport tourism leisure addr:city '
                           'addr:street addr:housenumber" '
                           '--csv-headline '
                           '--max-objects=1000000000 > new.csv'], shell=True)
    df = pd.read_csv('new.csv', sep='\t', error_bad_lines=False)
    df.head(100000).to_csv('osm_test.csv', index=False)
    df.name = df.name.astype(str)
    df = df.loc[df.name != 'nan']
    df['category'] = np.nan
    for column in columns:
        df['category'] = df['category'].fillna(df[column])
    df.category = df.category.astype(str)
    df = df.loc[df['category'] != 'nan']
    df.rename(columns={'@id': 'id', '@lon': 'lon', '@lat': 'lat', 'addr:city': 'city',
                       'addr:street': 'street', 'addr:housenumber': 'housenumber'}, inplace=True)
    df = df[['id', 'lon', 'lat', 'category', 'name', 'city', 'street', 'housenumber']]
    df.to_sql('osm_objects_nigeria', con=engine, schema='osm_objects', if_exists='replace')


if __name__ == '__main__':
    main()
