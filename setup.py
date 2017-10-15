from setuptools import setup
from pathlib import Path

from ai.backend.agent.version import VERSION


requires = [
    'ConfigArgParse',
    'coloredlogs>=5.2',
    'async_timeout>=1.1',
    'pyzmq>=16.0',
    'aiodocker',
    'aiozmq>=0.7',
    'aiohttp~=2.2.0',
    'aioredis>=0.2.8',
    'aiobotocore>=0.3.0',
    'aiotools>=0.4.5',
    'namedlist',
    'psutil',
    'requests',
    'requests_unixsocket',
    'simplejson',
    'uvloop>=0.8',
    'psutil>=5.2.2',
    'backend.ai-common~=1.0.3',
]
build_requires = [
    'wheel',
    'twine',
]
test_requires = [
    'pytest>=3.1',
    'pytest-asyncio',
    'pytest-cov',
    'pytest-mock',
    'asynctest',
    'flake8',
    'codecov',
]
dev_requires = build_requires + test_requires + [
    'pytest-sugar',
]
ci_requires = []
monitor_requires = [
    'datadog>=0.16.0',
    'raven>=6.1',
]


setup(
    name='backend.ai-agent',
    version=VERSION,
    description='Backend.AI Agent',
    long_description=Path('README.rst').read_text(),
    url='https://github.com/lablup/backend.ai-agent',
    author='Lablup Inc.',
    author_email='joongi@lablup.com',
    license='LGPLv3',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Environment :: No Input/Output (Daemon)',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development',
    ],

    packages=['ai.backend.agent'],

    python_requires='>=3.6',
    install_requires=requires,
    extras_require={
        'build': build_requires,
        'test': test_requires,
        'dev': dev_requires,
        'ci': ci_requires,
        'monitor': monitor_requires,
    },
)
