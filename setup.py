from setuptools import setup

setup(
    name='dry_pipe',
    version='0.0.7',
    packages=['dry_pipe'],
    install_requires=[
        'click==8.1.3',
        'uvicorn==0.17.6',
        'python-socketio==5.6.0',
        'psutil==5.9.1',
        'rich==12.4.4',
        'paramiko==2.11.0',
        'PyYAML==5.4.1',
        'requests==2.27.1'
    ],
    #package_data={'': ['ui/dist/main.js']},
    entry_points='''
        [console_scripts]
        drypipe=dry_pipe.cli:run_cli
    ''',
)