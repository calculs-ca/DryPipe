import glob
import os


def dag_gen_fileset_output(remote_task_conf, test_case):

    def dag_gen(dsl):

        t1 = dsl.task(
            key=f"t1",
            task_conf=remote_task_conf
        ).produces(
            out_files=dsl.file("f*.txt"),
            out_files_b=dsl.file("b*.txt"),
            z1=dsl.file("z1.txt"),
            z2=dsl.file("z2.txt")
        ).calls(f"""
            #!/usr/bin/env bash
            echo "1" > $__task_output_dir/f1.txt                           
            echo "2" > $__task_output_dir/f2.txt
            echo "3" > $__task_output_dir/f3.txt
            
            echo "a" > $__task_output_dir/b_1.txt                           
            echo "b" > $__task_output_dir/b_2.txt
                        
            echo "zaz1" > $z1
            echo "zaz2" > $z2
        """)()

        yield t1

    def validate(pipeline_instance):
        remote_task = pipeline_instance.tasks["t1"]

        test_case.assertEqual(remote_task.out.z1.load_as_string(), 'zaz1\n')
        test_case.assertEqual(remote_task.out.z2.load_as_string(), 'zaz2\n')

        out_dir = remote_task.v_abs_work_dir()
        fileset = sorted([
            os.path.basename(f) for f in glob.glob(os.path.join(out_dir, "f*.txt"))
        ])

        test_case.assertEqual(fileset, ["f1.txt", "f2.txt", "f3.txt"])

        with open(os.path.join(out_dir, "f2.txt")) as f2:
            test_case.assertEqual(f2.read(), "2\n")

        fileset_b = sorted([
            os.path.basename(f) for f in glob.glob(os.path.join(out_dir, "b*.txt"))
        ])

        test_case.assertEqual(fileset_b, ["b_1.txt", "b_2.txt"])

        with open(os.path.join(out_dir, "b_1.txt")) as f2:
            test_case.assertEqual(f2.read(), "a\n")

    return dag_gen, validate
