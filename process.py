#!/usr/bin/env python

import csv
import os
import sqlite3

import envoy


BIN_PATH = 'ark-tools/warts-aspaths'
OUTPUT_FIELDS = [
    'monitor_ip',
    'monitor_as',
    'dest_ip',
    'dest_as',
    'rtt',
    'ip_hops',
    'as_hops'
]

DB = None
CURSOR = None


def main():
    """
    Parse everything! Write results to a single output file.
    """
    global DB
    global CURSOR

    DB = sqlite3.connect('ark.db')

    DB.execute('PRAGMA main.synchronous=OFF')
    DB.execute('PRAGMA main.journal_mode=MEMORY')

    DB.execute('DROP TABLE IF EXISTS ark;')
    DB.execute('CREATE TABLE ark (monitor_ip TEXT, monitor_as INTEGER, dest_ip TEXT, dest_as INTEGER, rtt REAL, ip_hops INTEGER, as_hops INTEGER);')

    CURSOR = DB.cursor()

    d = {
        'year': 2014,
        'month': 3,
        'day': 19
    }

    parse_date(d)

    CURSOR.close()


def parse_date(d):
    """
    Parse all Ark files for a single day.
    """
    routing_path = 'data.caida.org/datasets/routing/routeviews-prefix2as/%(year)d/%(month)02d/routeviews-rv2-%(year)d%(month)02d%(day)02d-1200.pfx2as.gz' % d
    ark_root = 'data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2014/cycle-%(year)d%(month)02d%(day)02d/' % d

    for filename in os.listdir(ark_root):
        print(filename)
        ark_path = os.path.join(ark_root, filename)

        cmd = '%(bin)s -A %(routes)s %(warts)s' % {
            'bin': BIN_PATH,
            'routes': routing_path,
            'warts': ark_path
        }

        r = envoy.run(cmd)

        parse_ark(r.std_out)


def parse_ark(ark_text):
    """
    Parse Ark text format and stream results into a CSV writer.
    """
    monitor_ip = None
    monitor_as = None

    for line in ark_text.splitlines():
        if line[0] == '#':
            continue
        elif line[0] == 'T':
            continue

        fields = line.strip().split('\t')

        if line[0] == 'M':
            monitor_ip = fields[1]
            monitor_as = fields[2]
        else:
            row ={
                'monitor_ip': monitor_ip,
                'monitor_as': monitor_as,
                'dest_ip': fields[3],
                'dest_as': fields[5],
                'rtt': fields[2],
                'ip_hops': sum([int(f.split(':')[1]) for f in fields[6:]]),
                'as_hops': len(fields) - 6
            }

            CURSOR.execute('INSERT INTO ark VALUES (:monitor_ip, :monitor_as, :dest_ip, :dest_as, :rtt, :ip_hops, :as_hops)', row)


if __name__ == '__main__':
    main()
