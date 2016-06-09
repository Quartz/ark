# ark

Tools for processing traceroute data from CAIDA's [Ark project]().

Note: tools in the `ark-tools` folder were provided by Young Hyun from CAIDA.

## Setup

```
mkvirtualenv ark
pip install -r requirements.txt

cd ark-tools
gem install rb-asfinder-0.10.1.gem rb-wartslib-1.4.2.gem
cd ..
```

This project also requires a running, local instance of Postgres with a no-password user named `ark` who owns a database named `ark.`

## Sourcing the data

Note: this is a huge amount of data and will take a very long time to download:

```
./fetch.sh
```

You will also need to download the [MaxMind GeoLite2 Country database](http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz) and unzip it in the root directory as `GeoLite2-City.mmdb`.

## Building the database

```
python process.py
```

## Running queries

```
cat by_country.sql | psql -q ark
cat by_monitor.sql | psql -q ark
```
