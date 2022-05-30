from dry_pipe import DryPipe, cli


def crash_o_palooza(dsl):

    for a in ["a", "b"]:
        for i in [1, 2, 3]:

            err_msg = "-".join([
                a for z in range(i)
            ])
            yield dsl.task(
                key=f"t{a}.{i}"
            ).produces(
                out_file=dsl.file("nothing.txt")
            ).calls(f"""
                #!/usr/bin/env bash
                echo "{err_msg}" >&2
                exit 1                    
            """)()

    for t in dsl.with_completed_tasks("t1", "t2", "t3"):
        raise Exception(f"will never get here !")


def pipeline():
    return DryPipe.create_pipeline(crash_o_palooza)
