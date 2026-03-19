from setuptools import setup

RICH_PACKAGE_VERSION = 'rich==14.3.3'

setup(
    name='dry_pipe',
    version='0.8.0',
    packages=['dry_pipe'],
    include_package_data=True,
    install_requires=[
        RICH_PACKAGE_VERSION
    ],
    entry_points='''
        [console_scripts]
        drypipe=dry_pipe.cli:run_cli
    ''',
)
