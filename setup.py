from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='mongoprocessing',
      version='0.2.0',
      description='A simple library for developing workflows using MongoDB Change Streams.',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url='https://github.com/Immowelt/MongoProcessing',
      download_url='https://github.com/Immowelt/MongoProcessing/archive/0.2.0.tar.gz',
      keywords='mongodb changestream changestreams pipeline workflow',
      author='Immowelt AG',
      author_email='a.seipel@immowelt.de',
      license='Apache2',
      packages=['mongoprocessing'],
      zip_safe=False)
