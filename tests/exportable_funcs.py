import dry_pipe


@dry_pipe.DryPipe.python_call()
def test_func(r, i, f):

    with open(f) as _f:
        f_int = int(_f.read().strip())

        return {
            "var_result": r + i + f_int
        }

