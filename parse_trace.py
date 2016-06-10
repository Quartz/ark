#!/usr/bin/env python

import csv
import sys

import maxminddb


MAXMIND = maxminddb.open_database('GeoLite2-City.mmdb')


def main():
    as_lookup = load_asnames()

    trace = sys.argv[1]
    rows = []

    for pair in trace.split(','):
        row = {}

        row['ip'], row['asn'] = pair.split(':')

        if row['asn'] in ['r', 'q']:
            row['asn_name'], row['asn_country'] = None, None
        else:
            try:
                row['asn_name'], row['asn_country'] = as_lookup[row['asn']]
            except:
                print('No ASN data for %s' % row['asn'])
                row['asn_name'], row['asn_country'] = None, None

        if row['ip']:
            row.update(geocode(row['ip']))

        rows.append(row)

    with open('trace.csv', 'w') as f:
        writer = csv.DictWriter(sys.stdout, fieldnames=['ip', 'country', 'subdivision', 'city', 'lat', 'lng', 'asn', 'asn_name', 'asn_country'])
        writer.writeheader()
        writer.writerows(rows)


def load_asnames():
    """
    Format:

    AS0           -Reserved AS-, ZZ
    AS1           LVLT-1 - Level 3 Communications, Inc., US
    """
    as_lookup = {}

    with open('asnames.txt', encoding='latin-1') as f:
        for line in f:
            if '--No Registry Entry--' in line:
                continue

            asn = line[:14].strip().replace('AS', '')
            name, country = line[14:].strip().rsplit(',', 1)

            as_lookup[asn] = (name, country)

    return as_lookup


def geocode(ip):
    loc = MAXMIND.get(ip)
    data = {}

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

    return data


if __name__ == '__main__':
    main()
