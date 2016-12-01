from setuptools import setup,find_packages

setup(
  name = 'recast-celery',
  version = '0.0.1',
  packages = find_packages(),
  include_package_data = True,
  install_requires = [
    'fabric',
    'Celery',
    'redis',
    'socket.io-emitter',
    'requests',
    'glob2'
  ],
  dependency_links = [
    'https://github.com/lukasheinrich/socket.io-python-emitter/tarball/master#egg=socket.io-emitter-0.0.1'
  ]
)
