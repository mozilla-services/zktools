__version__ = '0.1'

import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

setup(name='zktools',
      version=__version__,
      description='Zookeeper Tools',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        ],
      keywords='zookeeper lock leader',
      author="Ben Bangert",
      author_email="bbangert@mozilla.com",
      url="",
      license="MPL",
      packages=find_packages(),
      test_suite="zktools.tests",
      include_package_data=True,
      zip_safe=False,
      tests_require=['pkginfo', 'Mock>=0.7', 'nose'],
      install_requires=[
          "zkpython>=0.4",
      ],
      entry_points="""
      """
)
