#!/usr/bin/bash

if [ -d 'dist' ] ; then
    rm -r dist
fi
if [ -d 'build' ] ; then
    rm -r build
fi
if [ -d 'amazon_photos.egg-info' ] ; then
    rm -r amazon_photos.egg-info
fi