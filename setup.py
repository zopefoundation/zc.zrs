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

    packages = ['zc', 'zc.zrs'],
    zip_safe = True,
    package_dir = {'':'src'},
    namespace_packages = ['zc'],
    install_requires = ['setuptools', 'ZODB3', 'zc.twisted'],
    )
