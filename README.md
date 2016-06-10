# ark

Tools for processing traceroute data from CAIDA's [Ark project](http://www.caida.org/projects/ark/).

Tools in the `ark-tools` folder were provided by CAIDA's [Young Hyun](http://www.caida.org/~youngh/).

## Setup

```
mkvirtualenv ark
pip install -r requirements.txt

cd ark-tools
gem install rb-asfinder-0.10.1.gem rb-wartslib-1.4.2.gem
cd ..   
```

This project also requires a running, local instance of Postgres with a no-password user named `ark` who owns a geo-enabled database named `ark`:

```
createdb -O ark ark
psql -q ark -c "CREATE EXTENSION postgis;"
```

## Sourcing the data

This script uses data from CAIDA's [Ark IPv4 Routed /24 Topology Dataset](http://www.caida.org/data/active/ipv4_routed_24_topology_dataset.xml). The following script will will download all data, for all three monitoring teams, for every day in March of 2014. Caution: **This is 87GB of data**. The script can be stopped and started without starting over.

```
./fetch.sh
```

You will also need to download the following files to the root project directory:

* [MaxMind GeoLite2 Country database](http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz) (unzip it)
* [BGP Reports mapping of AS numbers to names](http://bgp.potaroo.net/as6447/asnames.txt)

## Building the database

Warning: This script will run for around a half hour **per day of data**. If you're loading a month of data it could easily take a full day. (Assuming you even have the disk space to hold it all.)

```
./process.py
```

## Running queries

```
cat by_country.sql | psql -q ark
cat by_monitor.sql | psql -q ark
```

## Analyzing a trace path

You can pass a `trace` from the database into `parse_trace.py` to generate detailed path data in CSV format:

```
./parse_trace.py "216.66.30.102:6939,216.66.30.101:6939,213.248.67.125:1299,213.155.130.34:1299,157.130.60.13:701,:q,:r,108.51.141.48:701" > nyc_to_dc.csv
```
