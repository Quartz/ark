#!/usr/bin/env python

import csv

import maxminddb


# New Orleans, LA > Cook Islands
TRACE = 'r:2,22773:4,r:1,4323:2,60725:2,10131:1'


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


def main():
    as_lookup = load_asnames()

    rows = []

    for pair in TRACE.split(','):
        asn, hops = pair.split(':')

        if asn in ['r', 'q']:
            name, country = None, None
        else:
            try:
                name, country = as_lookup[asn]
            except:
                print('No ASN data for %s' % asn)
                name, country = None, None

        rows.append([asn, name, country, hops])

    with open('trace.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['asn', 'name', 'country', 'hops'])
        writer.writerows(rows)


if __name__ == '__main__':
    main()
