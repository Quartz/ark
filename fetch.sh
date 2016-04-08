#!/bin/bash

wget -r --no-parent -nc -R "index.html*" http://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2014/cycle-20140319/
wget -r --no-parent -nc -R "index.html*" http://data.caida.org/datasets/routing/routeviews-prefix2as/2014/03/
