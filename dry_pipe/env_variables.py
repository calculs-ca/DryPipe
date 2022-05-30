import os


def __env_var(name, default=None):

    name = f"DRYPIPE_{name}"

    if name in os.environ:
        return os.environ[name]

    return default


def log_config():
    return __env_var("LOG_CONFIG")


def test_remote_user():
    return __env_var("TEST_REMOTE_USER", "maxl")


def test_remote_host():
    return __env_var("TEST_REMOTE_HOST", "ip32.ccs.usherbrooke.ca")


def test_remote_ssh_key_path():
    return __env_var("TEST_REMOTE_SSH_SECRET_KEY_PATH", "/home/maxou/.ssh/id_rsa")


def test_remote_dir():
    #return __env_var("TEST_REMOTE_DIR", "/home/maxl/drypipe-tests")
    return __env_var("TEST_REMOTE_DIR", "/nfs3_ib/ip32-ib/home/maxl/drypipe-tests")


def test_remote_connection_string():
    return f"{test_remote_user()}@{test_remote_host()}:{test_remote_dir()}:{test_remote_ssh_key_path()}"


def test_slurm_group():
    return __env_var("TEST_SLURM_GROUP", "def-xroucou_cpu")