import os
from version import __version__
from setuptools import setup, find_packages

with open(os.path.sep.join(os.path.dirname(os.path.realpath(__file__)).split(os.path.sep)
                           + ["requirements.txt"]),
          "r", encoding="utf-8") as fh:
    requirements = fh.read().splitlines()

setup(
    name='ileads_lead_generation',
    
    # some version number you may wish to add - increment this after every update
    version=__version__,
    install_requires=requirements,
   
    packages=find_packages(),
    description='lead generation module',
    author='ileads',
)
