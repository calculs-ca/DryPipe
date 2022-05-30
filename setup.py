from setuptools import setup

minimal_requirements = [
    'click==8.1.3',
    'uvicorn==0.17.6',
    'python-socketio==5.6.0',
    'psutil==5.9.1',
    'rich==12.4.4',
    'requests==2.27.1'
]

extra_requirements = [
    'PyYAML==5.4.1',
    'paramiko==2.11.0'
]

setup(
    name='dry_pipe',
    version='0.0.9',
    packages=['dry_pipe'],
    install_requires=minimal_requirements,
    extras_require={"full": extra_requirements},
    #package_data={'': ['ui/dist/main.js']},
    entry_points='''
        [console_scripts]
        drypipe=dry_pipe.cli:run_cli
    ''',
)