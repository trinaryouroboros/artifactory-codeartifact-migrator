import artifactory_codeartifact_migrator

from setuptools import setup

setup(
  name='artifactory_codeartifact_migrator',
  version=artifactory_codeartifact_migrator.__version__,
  description='Artifactory to Codeartifact migrator',
  long_description='Migrate artifacts from JFrog Artifactory to AWS Codeartifact',
  packages=[
    'artifactory_codeartifact_migrator'
  ],
  package_data={},
  url='https://github.com/trinaryouroboros/artifactory_codeartifact_migrator',
  author='Shawn Qureshi',
  author_email='shawn_q@email.com',
  license='Apache',
  install_requires=[
    'requests',
    'requests_toolbelt',
    'boto3',
    'botocore',
    'dask',
    'twine',
    'jmespath',
    'multiprocess'
  ],
  python_requires='>=3',
  classifiers=[
    'Development Status :: 2 - Pre-Alpha',
    'Intended Audience :: Developers',
    'Topic :: Software Development',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3 :: Only',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10'
  ],
  keywords='cli artifactory_codeartifact_migrator artifactory codeartifact migrator migration artifacts artifact jfrog aws',
  entry_points={
    'console_scripts': [
      'artifactory-codeartifact-migrator = artifactory_codeartifact_migrator.__main__:main',
    ]
  }
)