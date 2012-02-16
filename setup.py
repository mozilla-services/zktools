__version__ = '0.2.1'

import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.rst')) as f:
    CHANGES = f.read()

reqs = [
    "clint>=0.3.0",
    "zc.zk>=0.7.0"
]

on_rtd = os.environ.get('READTHEDOCS', None) == 'True'
if not on_rtd:
    reqs.extend([
        "zc-zookeeper-static>=3.3.4.0",
    ])

setup(
    name='zktools',
    version=__version__,
    description='Zookeeper Tools',
    long_description=README + '\n\n' + CHANGES,
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
    ],
    keywords='zookeeper lock leader configuration',
    author="Mozilla Foundation",
    author_email="bbangert@mozilla.com",
    url="http://zktools.readthedocs.org/",
    license="MPLv2.0",
    packages=find_packages(),
    test_suite="zktools.tests",
    include_package_data=True,
    zip_safe=False,
    tests_require=['pkginfo', 'Mock>=0.7', 'nose'],
    install_requires=reqs,
    entry_points="""
    [console_scripts]
    zooky = zktools.locking:lock_cli

    """
)
