import os
from setuptools import setup

open('setup.cfg', 'w').write("""
[bdist_egg]
exclude-source-files = true
""")

open('LICENSE.txt', 'w').write(open('ZEL.txt').read())

entry_points = """
[console_scripts]
zrsmonitor-script = zc.zrs.monitor:main
"""

try:
    setup(
        name = 'zc.zrs_eval',
        version = open('version.txt').read().strip(),
        author = "Jim Fulton",
        author_email = "jim#zope.com",
        description = "Zope Replication Server",
        license = "Zope Evaluation License 1.0",
        keywords = "ZODB",

        packages = ['zc', 'zc.zrs'],
        include_package_data = True,
        data_files = [('.', ['README.txt', 'LICENSE.txt'])],
        zip_safe = True,
        entry_points = entry_points,
        package_dir = {'':'src'},
        namespace_packages = ['zc'],
        install_requires = ['setuptools', 'ZODB3', 'Twisted'],
        )
finally:
    os.remove('setup.cfg')
    os.remove('LICENSE.txt')
