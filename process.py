#!/usr/bin/env python

import csv
from datetime import date
import os
import psycopg2

import envoy
import maxminddb


BIN_PATH = 'ark-tools/simple_warts.rb'
MAXMIND = maxminddb.open_database('GeoLite2-City.mmdb')


def main():
    """
    Parse everything! Write results to a single output file.
    """
    db = psycopg2.connect(dbname='ark', user='ark')
    cursor = db.cursor()

    cursor.execute('DROP TABLE IF EXISTS monitors CASCADE;')
    cursor.execute('''CREATE TABLE monitors (
        name char(8) primary key,
        ip char(15),
        location varchar,
        lat real,
        lng real,
        asn char(12),
        org_class varchar,
        org_name varchar,
        geom GEOMETRY(Point, 4326)
    );
    ''')

    cursor.execute('''CREATE INDEX monitors_gix
        ON monitors
        USING GIST (geom);
    ''')

    # REFERENCES monitors (name)
    cursor.execute('DROP TABLE IF EXISTS traces;')
    cursor.execute('''CREATE TABLE traces (
        id serial primary key,
        probe_date date,
        monitor_name char(8),
        monitor_ip char(15),
        monitor_as varchar,
        dest_ip char(15),
        dest_as varchar,
        country varchar,
        subdivision varchar,
        city varchar,
        lat real,
        lng real,
        rtt real,
        ip_hops integer,
        as_hops integer,
        trace varchar,
        geom GEOMETRY(Point, 4326)
    );
    ''')

    cursor.execute('''CREATE INDEX traces_gix
        ON traces
        USING GIST (geom);
    ''')

    load_monitors(db)

    for day in range(1, 7):
        d = {
            'year': 2014,
            'month': 3,
            'day': day
        }

        print(d)

        parse_date(d, db)

    db.close()


def load_monitors(db):
    """
    Parse data on Ark monitors.
    """
    cursor = db.cursor()

    with open('ark-monitors-20160322.txt') as f:
        reader = csv.reader(f, delimiter='|')
        next(reader)

        for row in reader:
            data = ', '.join(['\'%s\'' % r for r in row])
            data += ', ST_GeomFromText(\'POINT(%s %s)\', 4326)' % (row[4], row[3])

            cursor.execute('''
                INSERT INTO monitors
                VALUES (%s)''' % data)

    db.commit()


def parse_date(d, db):
    """
    Parse all Ark files for a single day.
    """
    routing_path = 'data.caida.org/datasets/routing/routeviews-prefix2as/%(year)d/%(month)02d/routeviews-rv2-%(year)d%(month)02d%(day)02d-1200.pfx2as.gz' % d

    for team in range(1, 4):
        print('team-%i' % team)

        d['team'] = team

        ark_root = 'data.caida.org/datasets/topology/ark/ipv4/probe-data/team-%(team)i/2014/cycle-%(year)d%(month)02d%(day)02d/' % d

        if not os.path.exists(ark_root):
            continue

        for filename in os.listdir(ark_root):
            print(filename)

            ark_path = os.path.join(ark_root, filename)
            monitor_name = filename.split('.')[-3].strip()

            cmd = '%(bin)s %(routes)s %(warts)s' % {
                'bin': BIN_PATH,
                'routes': routing_path,
                'warts': ark_path
            }

            r = envoy.run(cmd)

            parse_ark(monitor_name, date(d['year'], d['month'], d['day']), r.std_out, db)


def parse_ark(monitor_name, probe_date, ark_text, db):
    """
    Parse Ark text format and stream results into a CSV writer.
    """
    cursor = db.cursor()

    monitors = []

    for line in ark_text.splitlines():
        fields = line.strip().split('\t')

        row = {
            'monitor_name': monitor_name,
            'probe_date': probe_date,
            'monitor_ip': fields[0],
            'monitor_as': fields[1],
            'dest_ip': fields[2],
            'dest_as': fields[3],
            'country': None,
            'subdivision': None,
            'city': None,
            'lat': None,
            'lng': None,
            'rtt': fields[4],
            'ip_hops': 0,
            'as_hops': 0,
            'trace': ','.join(fields[5:]),
            'geom': None
        }

        last_asn = None

        for field in fields[5:]:
            ip, asn = field.split(':')
            row['ip_hops'] += 1

            if asn in ['q', 'r', last_asn]:
                continue

            row['as_hops'] += 1
            last_asn = asn

        loc = MAXMIND.get(row['dest_ip'])

        if loc:
            if 'country' in loc:
                row['country'] = loc['country']['names']['en']

            if 'subdivisions' in loc:
                row['subdivision'] = loc['subdivisions'][0]['names']['en']

            if 'city' in loc:
                row['city'] = loc['city']['names']['en']

            if 'location' in loc:
                row['lat'] = loc['location']['latitude']
                row['lng'] = loc['location']['longitude']
                row['geom'] = 'POINT(%s %s)' % (row['lng'], row['lat']) if row['lng'] and row['lat'] else None

        cursor.execute('''
            INSERT INTO traces (probe_date, monitor_name, monitor_ip, monitor_as, dest_ip, dest_as, country, subdivision, city, lat, lng, rtt, ip_hops, as_hops, trace, geom)
            VALUES (%(probe_date)s, %(monitor_name)s, %(monitor_ip)s, %(monitor_as)s, %(dest_ip)s, %(dest_as)s, %(country)s, %(subdivision)s, %(city)s, %(lat)s, %(lng)s, %(rtt)s, %(ip_hops)s, %(as_hops)s, %(trace)s, ST_GeomFromText(%(geom)s, 4326));'''
        , row)

    db.commit()

if __name__ == '__main__':
    main()
