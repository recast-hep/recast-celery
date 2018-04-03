from setuptools import setup,find_packages

setup(
  name = 'wflow-backend',
  version = '0.0.1',
  packages = find_packages(),
  include_package_data = True,
  install_requires = [
    'pyyaml',
    'paramiko',
    'requests',
    'scp',
    'glob2'
  ],
  entry_points = {
      'console_scripts': [
          'wflow-process=wflowbackend.process:main',
          'wflow-process-server=wflowbackend.process_server:main',
      ],
  },)
