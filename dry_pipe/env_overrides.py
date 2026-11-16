import re
import shlex


class InvalidEnv(Exception):
    pass


def parse_env_option(text):
    """the NAME=VALUE pairs of --env, space separated, values with spaces are quoted"""

    def pair_of(token):
        name, sep, value = token.partition("=")
        if sep != "=" or re.fullmatch("[A-Za-z_][A-Za-z0-9_]*", name) is None:
            raise InvalidEnv(f"invalid --env '{text}', expected NAME=VALUE, got '{token}'")
        if "," in value or "|" in value:
            raise InvalidEnv(f"--env values can't contain ',' or '|', got '{token}'")
        return name, value

    try:
        return dict(pair_of(token) for token in shlex.split(text))
    except ValueError as e:
        raise InvalidEnv(f"invalid --env '{text}': {e}")


def encode_env_overrides(env_overrides):
    """the value of DRYPIPE_ENV_OVERRIDES, '|' separated, because sbatch's --export splits on ','"""
    return "|".join(f"{name}={value}" for name, value in env_overrides.items())


def decode_env_overrides(text):
    return dict(pair.partition("=")[::2] for pair in text.split("|") if pair != "")
