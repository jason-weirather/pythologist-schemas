from setuptools import setup, find_packages
from codecs import open
from os import path

this_folder = path.abspath(path.dirname(__file__))
with open(path.join(this_folder,'README.md'),encoding='utf-8') as inf:
  long_description = inf.read()

setup(
  name='pythologist_schemas',
  version='0.1.0',
  description='Check the assumptions of inputs for pythologist ahead of reading',
  long_description=long_description,
  test_suite='nose2.collector.collector',
  tests_require=['nose2'],
  url='https://github.com/jason-weirather/pythologist-schemas',
  author='Jason L Weirather',
  author_email='jason.weirather@gmail.com',
  license='Apache License, Version 2.0',
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Science/Research',
    'Topic :: Scientific/Engineering :: Bio-Informatics',
    'License :: OSI Approved :: Apache Software License'
  ],
  keywords='bioinformatics',
  packages=['pythologist_schemas',
            'schema_data',
            'schema_data.inputs',
            'schema_data.inputs.platforms.InForm'
            ],
  install_requires = ['jsonschema','importlib_resources'],
  include_package_data = True,
  entry_points = {
    'console_scripts':['pythologist-stage=pythologist_schemas.cli.stage_tool:cli',
                       'pythologist-templates=pythologist_schemas.cli.template_tool:cli',
                       'pythologist-run=pythologist_schemas.cli.run_tool:cli'
                      ]
  }
)
