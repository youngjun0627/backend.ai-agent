#! /bin/sh

find * -name '*.py[co]' -delete

pip install -U pip setuptools wheel
pip install -U -r requirements-ci.txt

python -B -m pytest
