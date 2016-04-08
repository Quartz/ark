#!/usr/bin/env python

import csv
import os

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


def main():
    config = {
        'year': 2014,
        'month': 3,
        'day': 19
    }

    routing_path = 'routing/routeviews-prefix2as/%(year)d/%(month)02d/routeviews-rv2-%(year)d%(month)02d%(day)02d-1200.pfx2as.gz' % config
    ark_root = 'ark/ipv4/probe-data/team-1/2014/cycle-%(year)d%(month)02d%(day)02d/' % config

    f = open('paths.csv', 'w')
    writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
    writer.writeheader()

    for filename in os.listdir(ark_root):
        ark_path = os.path.join(ark_root, filename)

        cmd = '%(bin)s -A %(routes)s %(warts)s' % {
            'bin': BIN_PATH,
            'routes': routing_path,
            'warts': ark_path
        }

        r = envoy.run(cmd)

        monitor_ip = None
        monitor_as = None

        for line in r.std_out.splitlines():
            if line[0] == '#':
                continue
            elif line[0] == 'T':
                continue

            fields = line.strip().split('\t')

            if line[0] == 'M':
                bits = line.split('\t')
                monitor_ip = bits[1]
                monitor_as = bits[2]
            else:
                writer.writerow({
                    'monitor_ip': monitor_ip,
                    'monitor_as': monitor_as,
                    'dest_ip': fields[3],
                    'dest_as': fields[5],
                    'rtt': fields[2],
                    'ip_hops': sum([int(f.split(':')[1]) for f in fields[6:]]),
                    'as_hops': len(fields) - 6
                })

    f.close()

if __name__ == '__main__':
    main()
