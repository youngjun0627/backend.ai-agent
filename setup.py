from setuptools import setup
from pathlib import Path
import re

here = Path(__file__).resolve().parent


def _get_version():
    root_src = (here / 'sorna' / 'agent' / '__init__.py').read_text()
    try:
        version = re.findall(r"^__version__ = '([^']+)'\r?$", root_src, re.M)[0]
    except IndexError:
        raise RuntimeError('Unable to determine myself version.')
    return version


setup(
    name='sorna-agent',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version=_get_version(),
    description='Sorna agent',
    long_description='',
    url='https://github.com/lablup/sorna-agent',
    author='Lablup Inc.',
    author_email='joongi@lablup.com',
    license='LGPLv3',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Environment :: No Input/Output (Daemon)',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development',
    ],

    packages=['sorna.agent'],
    namespace_packages=['sorna'],

    python_requires='>=3.6',
    install_requires=[
        'coloredlogs>=5.2',
        'async_timeout>=1.1',
        'pyzmq>=16.0',
        'aiozmq>=0.7',
        'aiohttp>=1.1',
        'aiodocker',
        'aioredis',
        'aiobotocore',
        'msgpack-python',
        'namedlist',
        'simplejson',
        'uvloop>=0.7',
        'sorna-common>=0.8,<0.9',
    ],
    extras_require={
        'dev': ['pytest', 'pytest-asyncio', 'flake8', 'pep8-naming'],
        'test': ['pytest', 'pytest-asyncio'],
    },
    dependency_links=[
        'git+ssh://git@github.com/achimnol/aiodocker@master#egg=aiodocker-dev',
    ],
    package_data={
    },
    data_files=[],
)
