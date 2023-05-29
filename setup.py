import os
from version import __version__
from setuptools import setup, find_packages

with open(os.path.sep.join(os.path.dirname(os.path.realpath(__file__)).split(os.path.sep)
                           + ["requirements.txt"]),
          "r", encoding="utf-8") as fh:
    requirements = fh.read().splitlines()

setup(
    # this will be the package name you will see, e.g. the output of 'conda list' in anaconda prompt
    name='ileads_lead_generation',
    # some version number you may wish to add - increment this after every update
    version=__version__,
    install_requires=requirements,
    # Use one of the below approach to define package and/or module names:

    # if there are only handful of modules placed in root directory, and no packages/directories exist then can use
    # below syntax packages=[''], #have to import modules directly in code after installing this wheel, like import
    # mod2 (respective file name in this case is mod2.py) - no direct use of distribution name while importing

    # can list down each package names - no need to keep __init__.py under packages / directories packages=['<list of
    # name of packages>'], #importing is like: from package1 import mod2, or import package1.mod2 as m2

    # this approach automatically finds out all directories (packages) - those must contain a file named __init__.py
    # (can be empty)
    #package_dir={"": 'ileads_lead_generation'},
    #packages=find_packages(where='ileads_lead_generation'),
    packages=find_packages(),
    description='lead generation module',
    author='ileads',
)
