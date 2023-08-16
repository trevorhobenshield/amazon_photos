#!/usr/bin/bash

bash clean.sh
python -m build
python -m twine upload dist/*
bash clean.sh