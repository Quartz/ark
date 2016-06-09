#!/bin/bash

for i in $(seq -f "%02g" 1 31);
do
    for t in $(seq 1 3);
    do
        wget -r --no-parent -nc -R "index.html*" http://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-$t/2014/cycle-201403$i/
    done
done

wget -r --no-parent -nc -R "index.html*" http://data.caida.org/datasets/routing/routeviews-prefix2as/2014/03/
