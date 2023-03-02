from setuptools import setup

minimal_requirements = [
    'click==8.1.3',
    'uvicorn==0.17.6',
    'python-socketio==5.6.0',
    'rich==12.6.0',
    'requests==2.27.1',
    'readchar==4.0.3',
    'parallel-ssh==2.12.0'
]


setup(
    name='dry_pipe',
    version='0.7.45',
    packages=['dry_pipe'],
    install_requires=minimal_requirements,
    include_package_data=True,
    entry_points='''
        [console_scripts]
        drypipe=dry_pipe.cli:run_cli
    ''',
)