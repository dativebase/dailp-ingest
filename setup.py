import os
from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme:
    README = readme.read()


# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))


setup(
    name='dailp-ingest',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    description='Scripts to ingest DAILP Cherokee data into an OLD instance.',
    long_description=README,
    author='Lambda Bar Software Inc.',
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    install_requires=[
        'requests',
    ],
    entry_points={
        'console_scripts': [
            'dailp-ingest = ingest.ingest:main',
        ],
    },
)
