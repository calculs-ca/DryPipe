import os

from setuptools import setup

minimal_requirements = [
    'click==8.1.3',
    'uvicorn==0.17.6',
    'python-socketio==5.6.0',
    'psutil==5.9.1',
    'rich==12.6.0',
    'requests==2.27.1',
    'sshkeyboard==2.3.1'
]

extra_requirements = [
    'parallel-ssh==2.12.0'
]

is_full_req = os.environ.get("dry_pipe_is_full_req") == "True"

if is_full_req:
    minimal_requirements = minimal_requirements + extra_requirements

setup(
    name='dry_pipe',
    version='0.3.1',
    packages=['dry_pipe'],
    install_requires=minimal_requirements,
    extras_require={"full": extra_requirements},
    include_package_data=True,
    entry_points='''
        [console_scripts]
        drypipe=dry_pipe.cli:run_cli
    ''',
)