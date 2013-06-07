#! /usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="grid-polygins",
    version="0.1",
    author="Rory McCann",
    author_email="rory@technomancy.org",
    py_modules=['grid_polygons'],
    entry_points = {
        'console_scripts': [
            'grid-polygons = grid_polygons:main',
            ]
        },
    )
