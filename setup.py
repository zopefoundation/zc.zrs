from setuptools import setup, find_packages

name = 'zc.zrs'
setup(
    name = name,
    version = "0.1",
    author = "Jim Fulton",
    author_email = "jim#zope.com",
    description = "Zope Replication Server",
    license = "ZPL 2.1",
    keywords = "ZODB",

    packages = find_packages('src'),
    include_package_data = True,
    package_dir = {'':'src'},
    namespace_packages = ['zc'],
    install_requires = ['setuptools', 'zope.testing', 'ZODB3'],
    )
