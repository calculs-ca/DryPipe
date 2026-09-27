

Pipelines are "https://en.wikipedia.org/wiki/Programming_in_the_large_and_programming_in_the_small"

Pipelines, by virtue of being "bunch of programs wired up together", tend to have a lots of configuration

Everything runs in a single virtualenv


TaskConf(
    ...
    extra_env={
        "PYTHONPATH": "/a/b/c:/x/y:$__pipeline_code_dir",
        "A": "4321"
    }
)