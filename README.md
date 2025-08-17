# gotsport-toolbox

## environment

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip

## process

1. source .venv/bin/activate
1. python3 ./arbiter_etl/arbiter_import.py

1. create a folder for the season in /data, e.g. fall2025
1. create subfolders: import, export, lookup, templates
1. download schedule from GS
1. download template from arbiter


1. copy from export to template and manually fix
