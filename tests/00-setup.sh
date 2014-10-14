#!/bin/sh

sudo add-apt-repository -y ppa:juju/stable
sudo apt-get update
sudo apt-get install -y \
    amulet \
    python-flake8 \
    python-fixtures \
    python-jinja2 \
    python-mocker \
    python-psycopg2 \
    python-testtools \
    python-twisted-core \
    python-yaml \
    pgtune
