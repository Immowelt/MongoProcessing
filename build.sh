#!/usr/bin/env bash
rm dist/*
python setup.py sdist
sudo /opt/anaconda/bin/pip install --upgrade dist/*