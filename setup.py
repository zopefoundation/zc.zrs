name = 'zc.zrs'
version = open('version.txt').read().strip()

from setuptools import setup, find_packages

entry_points = """
[console_scripts]
zrsmonitor-script = zc.zrs.monitor:main
"""

setup(
    name = name,
    version = version,
    author = "Jim Fulton",
    author_email = "jim#zope.com",
    description = "Zope Replication Server",
    license = "ZVSL 1.0",
    keywords = "ZODB",

    packages = ['zc', 'zc.zrs'],
    include_package_data = True,
    zip_safe = True,
    entry_points = entry_points,
    package_dir = {'':'src'},
    namespace_packages = ['zc'],
    install_requires = [
        'setuptools',
        'ZODB3',
        'Twisted',
        ],
    )
