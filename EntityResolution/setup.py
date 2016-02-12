from distutils.core import setup
from setuptools import Extension,find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

print("testingggggggg!!")
print(find_packages())
setup(
  name = 'EntityResolution',
  version = '1.0.0',
  description = 'Record linkage using reference dictionary for entities',
  long_description=long_description,
  author = 'Majid Ghasemi Gol',
  author_email = 'ghasemig@usc.edu',
  url = '', # use the URL to the github repo
  packages = find_packages(),
  keywords = ['heap', 'entity_extraction'] # arbitrary keywords
)
