from dry_pipe.core_lib import SlurmArrayParentTask

if __name__ == '__main__':
    SlurmArrayParentTask.cli(['run', '--pipeline-instance-dir', 'z', '--task-key', 'a'])
