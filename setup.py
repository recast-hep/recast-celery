from setuptools import setup,find_packages

setup(
  name = 'wflow-celery',
  version = '0.0.1',
  packages = find_packages(),
  include_package_data = True,
  install_requires = [
    'pyyaml',
    'fabric',
    'Celery',
    'redis',
    'requests',
    'scp',
    'glob2'
  ],
  entry_points = {
      'console_scripts': [
          'wflow-process=wflowcelery.process:main',
      ],
  },)
