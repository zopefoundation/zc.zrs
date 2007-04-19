import os
from setuptools import setup

open('setup.cfg', 'w').write("""
[bdist_egg]
exclude-source-files = true
""")

open('LICENSE.txt', 'w').write(open('ZBL.txt').read())

try:
    setup(
        name = 'zc.zrs_binary',
        version = open('version.txt').read().strip(),
        author = "Jim Fulton",
        author_email = "jim#zope.com",
        description = "Zope Replication Server",
        license = "Zope Binary License 1.0",
        keywords = "ZODB",

        packages = ['zc', 'zc.zrs'],
        include_package_data = True,
        data_files = [('.', ['README.txt', 'LICENSE.txt'])],
        zip_safe = True,
        package_dir = {'':'src'},
        namespace_packages = ['zc'],
        install_requires = ['setuptools', 'ZODB3', 'zc.twisted'],
        )
finally:
    os.remove('setup.cfg')
    os.remove('LICENSE.txt')
