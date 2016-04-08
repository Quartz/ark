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

## Usage

```
./fetch.sh
python process.py
```
