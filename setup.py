"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='task-utils',
    version='0.0.0',
    description='task-utils',
    long_description=long_description,
    # url="https://github.com/Foo/foo",
    author="Clemens Koza",
    author_email="koza@pria.at",
    license="AGPLv3+",

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],

    keywords='asyncio',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=[],

    # You can install these using the following syntax, for example:
    # $ pip install -e .[dev,test]
    extras_require={
        'test': ['pytest', 'pytest-runner', 'pytest-asyncio', 'pytest-cov', 'mypy'],
    },

    # package_data={
    #     'bar': ['*.ext'],
    # },

    entry_points={
        'console_scripts': [
        ],
    },
)
