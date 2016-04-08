#!/usr/bin/env python

import csv
from datetime import date
import os
import psycopg2

import envoy


BIN_PATH = 'ark-tools/warts-aspaths'


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
        org_name varchar);
    ''')

    # REFERENCES monitors (name)
    cursor.execute('DROP TABLE IF EXISTS traces;')
    cursor.execute('''CREATE TABLE traces (
        id serial primary key,
        probe_date date,
        monitor_name char(8),
        monitor_ip char(15),
        dest_ip char(15),
        rtt real,
        ip_hops integer,
        as_hops integer,
        trace varchar);
    ''')

    load_monitors(db)

    d = {
        'year': 2014,
        'month': 3,
        'day': 19
    }

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

            cursor.execute('''
                INSERT INTO monitors
                VALUES (%s)''' % data)

    db.commit()

def parse_date(d, db):
    """
    Parse all Ark files for a single day.
    """
    routing_path = 'data.caida.org/datasets/routing/routeviews-prefix2as/%(year)d/%(month)02d/routeviews-rv2-%(year)d%(month)02d%(day)02d-1200.pfx2as.gz' % d
    ark_root = 'data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2014/cycle-%(year)d%(month)02d%(day)02d/' % d

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

        parse_ark(monitor_name, date(d['year'], d['month'], d['day']), r.std_out, db)


def parse_ark(monitor_name, probe_date, ark_text, db):
    """
    Parse Ark text format and stream results into a CSV writer.
    """
    cursor = db.cursor()

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

            row = {
                'monitor_name': monitor_name,
                'probe_date': probe_date,
                'monitor_ip': monitor_ip,
                'dest_ip': fields[3],
                'rtt': fields[2],
                'ip_hops': ip_hops,
                'as_hops': as_hops,
                'trace': ','.join(fields[6:])
            }

            cursor.execute('''
                INSERT INTO traces (probe_date, monitor_name, monitor_ip, dest_ip, rtt, ip_hops, as_hops, trace)
                VALUES (%(probe_date)s, %(monitor_name)s, %(monitor_ip)s, %(dest_ip)s, %(rtt)s, %(ip_hops)s, %(as_hops)s, %(trace)s)'''
            , row)

    db.commit()

if __name__ == '__main__':
    main()
