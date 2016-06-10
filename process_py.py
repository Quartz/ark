#!/usr/bin/env python

import csv
from datetime import date
import os
import psycopg2

import envoy
import maxminddb
from scamper.sc_warts import WartsReader


BIN_PATH = 'ark-tools/warts-aspaths'
MAXMIND = maxminddb.open_database('GeoLite2-City.mmdb')


class ArkLoader(object):
    def __init__(self, db_name, db_user):
        self._db_name = db_name
        self._db_user = db_user

        self._db = None
        self._as_lookup = {}

    def run(self):
        self._setup_db()
        self._load_monitors()
        self._load_asnames()

        for day in range(1, 7):
            d = {
                'year': 2014,
                'month': 3,
                'day': day
            }

            print(d)

            self._parse_date(d)

        self._db.close()
        self._db = None

    def _setup_db(self):
        self._db = psycopg2.connect(dbname='ark_py', user='ark')
        cursor = self._db.cursor()

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
            dest_ip char(15),
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

    def _load_monitors(self):
        """
        Parse data on Ark monitors.
        """
        cursor = self._db.cursor()

        with open('ark-monitors-20160322.txt') as f:
            reader = csv.reader(f, delimiter='|')
            next(reader)

            for row in reader:
                data = ', '.join(['\'%s\'' % r for r in row])
                data += ', ST_GeomFromText(\'POINT(%s %s)\', 4326)' % (row[4], row[3])

                cursor.execute('''
                    INSERT INTO monitors
                    VALUES (%s)''' % data)

        self._db.commit()

    def _load_asnames(self):
        """
        Format:

        AS0           -Reserved AS-, ZZ
        AS1           LVLT-1 - Level 3 Communications, Inc., US
        """
        self._as_lookup = {}

        with open('asnames.txt', encoding='latin-1') as f:
            for line in f:
                if '--No Registry Entry--' in line:
                    continue

                asn = line[:14].strip().replace('AS', '')
                name, country = line[14:].strip().rsplit(',', 1)

                self._as_lookup[asn] = (name, country)

    def _parse_date(self, d):
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

                cmd = '%(bin)s -A %(routes)s %(warts)s' % {
                    'bin': BIN_PATH,
                    'routes': routing_path,
                    'warts': ark_path
                }

                r = envoy.run(cmd)

                self._parse_ark(monitor_name, date(d['year'], d['month'], d['day']), r.std_out)

    def _parse_ark(self, monitor_name, probe_date, ark_text):
        """
        Parse Ark text format and stream results into a CSV writer.
        """
        cursor = self._db.cursor()

        monitor_ip = None

        for line in ark_text.splitlines():
            if line[0] == '#':
                continue
            elif line[0] == 'T':
                continue

            fields = line.strip().split('\t')

            if line[0] == 'M':
                monitor_ip = fields[1]
            else:
                ip_hops = 0
                as_hops = 0
                last_asn = None

                for field in fields[6:]:
                    asn, ips = field.split(':')
                    ip_hops += int(ips)

                    if asn in ['q', 'r', last_asn]:
                        continue

                    as_hops += 1
                    last_asn = asn

                loc = MAXMIND.get(fields[3])
                country = None
                subdivision = None
                city = None
                lat = None
                lng = None

                if loc:
                    if 'country' in loc:
                        country = loc['country']['names']['en']

                    if 'subdivisions' in loc:
                        subdivision = loc['subdivisions'][0]['names']['en']

                    if 'city' in loc:
                        city = loc['city']['names']['en']

                    if 'location' in loc:
                        lat = loc['location']['latitude']
                        lng = loc['location']['longitude']

                row = {
                    'monitor_name': monitor_name,
                    'probe_date': probe_date,
                    'monitor_ip': monitor_ip,
                    'dest_ip': fields[3],
                    'country': country,
                    'subdivision': subdivision,
                    'city': city,
                    'lat': lat,
                    'lng': lng,
                    'rtt': fields[2],
                    'ip_hops': ip_hops,
                    'as_hops': as_hops,
                    'trace': ','.join(fields[6:]),
                    'geom': 'POINT(%s %s)' % (lng, lat) if lng and lat else None
                }

                cursor.execute('''
                    INSERT INTO traces (probe_date, monitor_name, monitor_ip, dest_ip, country, subdivision, city, lat, lng, rtt, ip_hops, as_hops, trace, geom)
                    VALUES (%(probe_date)s, %(monitor_name)s, %(monitor_ip)s, %(dest_ip)s, %(country)s, %(subdivision)s, %(city)s, %(lat)s, %(lng)s, %(rtt)s, %(ip_hops)s, %(as_hops)s, %(trace)s, ST_GeomFromText(%(geom)s, 4326));'''
                , row)

        self._db.commit()

if __name__ == '__main__':
    loader = ArkLoader('ark_py2', 'ark')
    loader.run()