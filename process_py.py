#!/usr/bin/env python

"""
Pure-Python parser: too slow to use!
"""

import csv
from datetime import date
import gzip
from io import TextIOWrapper
import os
from pprint import pprint

import envoy
import maxminddb
import psycopg2
import pytricia
from scamper.sc_warts import WartsReader


BIN_PATH = 'ark-tools/warts-aspaths'
MAXMIND = maxminddb.open_database('GeoLite2-City.mmdb')


class ArkLoader(object):
    def __init__(self, db_name, db_user):
        self._db_name = db_name
        self._db_user = db_user

        self._db = None
        self._as_trie = None
        self._as_lookup = None

    def run(self):
        self._setup_db()
        self._load_monitors()
        self._load_as_trie()
        self._load_as_names()

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
            probe_team integer,
            monitor_name char(8),
            monitor_ip char(15),
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

    def _load_monitors(self):
        """
        Parse data on Ark monitors.
        """
        print('Loading monitors')

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

    def _load_as_trie(self):
        """
        This script parses BGP routeviews data (as captured by Ark) into a
        prefix > AS map.

        http://data.caida.org/datasets/routing/routeviews-prefix2as/
        """
        print('Loading AS trie')

        self._as_trie = pytricia.PyTricia()

        with gzip.open('data.caida.org/datasets/routing/routeviews-prefix2as/2014/03/routeviews-rv2-20140302-1200.pfx2as.gz', 'r') as f:
            reader = csv.reader(TextIOWrapper(f), delimiter='\t')

            for prefix, bits, asn in reader:
                key = '%s/%s' % (prefix, bits)
                self._as_trie[key] = asn

    def _get_asn(self, ip):
        """
        Derive an ASN from an IP using the trie map.
        """
        try:
            return self._as_trie[ip]
        except KeyError:
            if not ip:
                return 'q'
            else:
                return 'r'

    def _load_as_names(self):
        """
        Format:

        AS0           -Reserved AS-, ZZ
        AS1           LVLT-1 - Level 3 Communications, Inc., US
        """
        print('Loading AS names')

        self._as_names = {}

        with open('asnames.txt', encoding='latin-1') as f:
            for line in f:
                if '--No Registry Entry--' in line:
                    continue

                asn = line[:14].strip().replace('AS', '')
                name, country = line[14:].strip().rsplit(',', 1)

                self._as_names[asn] = (name, country)

    def _parse_date(self, d):
        """
        Parse all Ark files for a single day.
        """
        print('Parsing!')

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

                reader = WartsReader(ark_path)

                while True:
                    (flags, hops) = reader.next()

                    if not flags:
                        break

                    self._parse_ark(monitor_name, d, flags, hops)

    def _parse_ark(self, monitor_name, meta, flags, hops):
        """
        Parse Ark text format and stream results into a CSV writer.
        """
        cursor = self._db.cursor()

        monitor_ip = flags['srcaddr']
        probe_date = date(meta['year'], meta['month'], meta['day'])
        probe_team = meta['team']
        dest_ip = flags['dstaddr']
        dest_as = self._get_asn(flags['dstaddr'])
        total_rtt = hops[-1]['rtt']

        last_asn = None
        as_hops = 0

        trace = []

        for hop in hops:
            hop_ip = hop['addr']
            asn = self._get_asn(hop_ip)

            trace.append((hop_ip, asn))

            if asn != last_asn:
                as_hops += 1

            last_asn = asn

        row = {
            'monitor_name': monitor_name,
            'probe_date': probe_date,
            'probe_team': probe_team,
            'monitor_ip': monitor_ip,
            'dest_ip': dest_ip,
            'dest_as': dest_as,
            'rtt': total_rtt,
            'ip_hops': len(trace),
            'as_hops': as_hops,
            'trace': ','.join([':'.join(t) for t in trace])
        }

        row.update(self._geocode(dest_ip))

        cursor.execute('''
            INSERT INTO traces (probe_date, probe_team, monitor_name, monitor_ip, dest_ip, dest_as, country, subdivision, city, lat, lng, rtt, ip_hops, as_hops, trace, geom)
            VALUES (%(probe_date)s, %(probe_team)s, %(monitor_name)s, %(monitor_ip)s, %(dest_ip)s, %(dest_as)s, %(country)s, %(subdivision)s, %(city)s, %(lat)s, %(lng)s, %(rtt)s, %(ip_hops)s, %(as_hops)s, %(trace)s, ST_GeomFromText(%(geom)s, 4326));'''
        , row)

        self._db.commit()

    def _geocode(self, ip):
        """
        Geocode a given IP using the MaxMind city database.
        """
        loc = MAXMIND.get(ip)

        data = {
            'country': None,
            'subdivision': None,
            'city': None,
            'lat': None,
            'lng': None,
            'geom': None
        }

        if loc:
            if 'country' in loc:
                data['country'] = loc['country']['names']['en']

            if 'subdivisions' in loc:
                data['subdivision'] = loc['subdivisions'][0]['names']['en']

            if 'city' in loc:
                data['city'] = loc['city']['names']['en']

            if 'location' in loc:
                data['lat'] = loc['location']['latitude']
                data['lng'] = loc['location']['longitude']

                data['geom'] = 'POINT(%(lng)s %(lat)s)' % data

        return data

if __name__ == '__main__':
    loader = ArkLoader('ark_py2', 'ark')
    loader.run()
