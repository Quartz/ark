#!/usr/bin/env python

"""
This script parses BGP routeviews data (as captured by Ark) into a
prefix > AS map.
"""

import csv
import gzip
from io import TextIOWrapper

import pytricia


TEST = [
    '130.60.40.26',
    '202.118.7.140',
    '84.88.81.122',
    '218.241.107.98',
    '195.206.248.254',
    '192.168.1.216',
    '194.68.13.6',
    '85.31.196.71'
]


def load_trie():
    pyt = pytricia.PyTricia()

    with gzip.open('data.caida.org/datasets/routing/routeviews-prefix2as/2014/03/routeviews-rv2-20140302-1200.pfx2as.gz', 'r') as f:
        reader = csv.reader(TextIOWrapper(f), delimiter='\t')

        for prefix, bits, asn in reader:
            key = '%s/%s' % (prefix, bits)
            pyt[key] = asn

    return pyt


def main():
    pyt = load_trie()

    for ip in TEST:
        print(ip, pyt.get(ip, 'r'))


if __name__ == '__main__':
    main()
