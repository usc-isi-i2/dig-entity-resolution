from distutils.core import setup
from setuptools import Extension,find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
  name = 'faerie',
  version = '1.0.0',
  description = 'Dictionary-based entity extraction with efficient filtering',
  long_description=long_description,
  author = 'Zheng Tang',
  author_email = 'zhengtan@isi.edu',
  url = 'https://github.com/ZhengTang1120/entity_extraction/', # use the URL to the github repo
  packages = find_packages(),
  keywords = ['heap', 'entity_extraction'], # arbitrary keywords
  ext_modules=[Extension(
        'singleheap',
        ['singleheap.c'])],
  install_requires=['nltk']
)