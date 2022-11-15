

def create_pipeline(remote_task_conf):

    def _pipeline_generator(dsl):
            t1 = dsl.task(
                key=f"t1",
                task_conf=remote_task_conf
            ).produces(
                x=dsl.var(int)
            ).calls(f"""
                #!/usr/bin/env bash
                echo "x={1}" > $__control_dir/output_vars                           
            """)()

            def t_i(i, prev_t):
                return dsl.task(
                    key=f"t{i}",
                    task_conf=remote_task_conf
                ).consumes(
                    i=prev_t.out.x
                ).produces(
                    x=dsl.var(int)
                ).calls(f"""
                    #!/usr/bin/env bash
                    echo "x={i}" > $__control_dir/output_vars                           
                """)()

            c = 1
            t = t1
            while c < 10:
                c += 1
                yield t
                t = t_i(c, t)

            for all_tasks in dsl.wait_for_matching_tasks("t*"):
                res = {
                    t.out.x.fetch() for t in all_tasks.tasks
                }
                if res != {1, 2, 3, 4, 5, 6, 7, 8, 9}:
                    raise Exception(f"bad results {res}")
                else:
                    print("over..")

    def pipeline_generator(dsl):

        def ta(i):
            return dsl.task(
                key=f"t{i}",
                task_conf=remote_task_conf
            ).produces(
                x=dsl.var(int)
            ).calls(f"""
                #!/usr/bin/env bash
                echo "x={i}" > $__control_dir/output_vars                           
            """)()

        t = ta(1)
        yield t
        for _ in dsl.wait_for_tasks(t):
            t = ta(2)
            yield t
            for _ in dsl.wait_for_tasks(t):
                t = ta(3)
                yield t
                for _ in dsl.wait_for_tasks(t):
                    t = ta(4)
                    yield t
                    for _ in dsl.wait_for_tasks(t):
                        t = ta(5)
                        yield t
                        for _ in dsl.wait_for_tasks(t):
                            t = ta(6)
                            yield t
                            for _ in dsl.wait_for_tasks(t):
                                t = ta(7)
                                yield t
                                for _ in dsl.wait_for_tasks(t):
                                    t = ta(8)
                                    yield t
                                    for _ in dsl.wait_for_tasks(t):
                                        t = ta(9)
                                        yield t
                                        for _ in dsl.wait_for_tasks(t):
                                            for all_tasks in dsl.wait_for_matching_tasks("t*"):
                                                res = {
                                                    t.out.x.fetch() for t in all_tasks.tasks
                                                }
                                                if res != {1, 2, 3, 4, 5, 6, 7, 8, 9}:
                                                    raise Exception(f"bad results {res}")

    return pipeline_generator