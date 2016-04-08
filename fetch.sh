#!/bin/bash

for i in $(seq -f "%02g" 1 31);
do
    wget -r --no-parent -nc -R "index.html*" http://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2014/cycle-201403$i/
done

wget -r --no-parent -nc -R "index.html*" http://data.caida.org/datasets/routing/routeviews-prefix2as/2014/03/
