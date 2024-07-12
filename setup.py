from setuptools import setup



setup(
    name='dry_pipe',
    version='0.8.0',
    packages=['dry_pipe'],
    include_package_data=True,
    entry_points='''
        [console_scripts]
        drypipe=dry_pipe.cli:run_cli
    ''',
)
