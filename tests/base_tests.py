import os
import sys
import pathlib
import shutil
import time
import unittest

import pipeline_with_single_bash_task
import test_helpers
from dry_pipe.internals import ValidationError, ProducedFile
from dry_pipe import DryPipe, TaskConf, DryPipeDsl
from test_01_simple_static_pipeline.simple_static_pipeline import simple_static_pipeline, \
    run_and_validate_pipeline_execution
from test_utils import TestSandboxDir, copy_pre_existing_file_deps_from_code_dir, test_suite_base_dir


def simple_static_pipeline_01_code_dir():
    d = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(d, "test_01_simple_static_pipeline")


def test_containers_dir():
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "containers"
    )

"""
def create_and_reset_simple_static_pipeline_instance(task_conf=None, containers_dir=None):
    pipeline_instance = DryPipe.create_pipeline(simple_static_pipeline).create_pipeline_instance(
        containers_dir=containers_dir or test_containers_dir(),
        task_conf=task_conf
    )
    pipeline_instance.annihilate_work_dirs()

    pipeline_instance.init_work_dir_if_not_exists()

    return pipeline_instance
"""

class BaseTests(unittest.TestCase):

    def ensure_validation_error(self, func, code=None):

        try:
            func()
            raise Exception("ValidationError was not raised")
        except ValidationError as e:
            if code is None or e.code is None:
                return
            if code != e.code:
                raise Exception(f"Wrong ValidationError.code: {e.code}, expected {code} \n{e}")

    def ensure_exception_thrown(self, func, code=None):

        try:
            func()
            raise Exception("Exception was not raised")
        except Exception as e:
            if code is None or e.code is None:
                return

    def test_task_construction_sanity(self):
        dsl = DryPipeDsl()

        self.ensure_validation_error(lambda: dsl.val([]))
        self.ensure_validation_error(lambda: dsl.file(123))


    def test_depends_on_sanity(self):
        dsl = DryPipeDsl()

        self.ensure_validation_error(
            lambda: dsl.task(key="t1").consumes(123),
            ValidationError.consumes_has_bad_positional_arg
        )

        self.ensure_validation_error(
            lambda: dsl.task(key="t1").consumes(dsl.val(123)),
            ValidationError.consumes_has_bad_positional_arg_val
        )

        self.ensure_validation_error(
            lambda: dsl.task(key="t1").consumes(x=123),
            ValidationError.consumes_has_invalid_kwarg_type
        )

        dsl.task(key="t1").consumes(x=dsl.val(123))

        dsl.task(key="t1").consumes(x=dsl.file("a123.txt"))


    def test_produces_sanity(self):

        dsl = DryPipeDsl()
        self.ensure_validation_error(
            lambda: dsl.task(key="t1").produces(123),
            ValidationError.produces_cant_take_positional_args
        )


        dsl.task(key="t1").produces(x=dsl.var(123))


        dsl.task(key="t1").produces(x=dsl.file("a.txt"))

    def test_referring_to_produced_files(self):

        dsl = DryPipeDsl()

        t1 = dsl.task("t1").produces(f1=dsl.file("f.txt")).calls("a.sh")()

        self.ensure_validation_error(lambda: t1.out.missing)

        # should not blow up:
        f1 = t1.out.f1

        self.ensure_exception_thrown(lambda: t1.f1)



        self.assertIsInstance(f1, ProducedFile)

        self.assertEqual("f.txt", f1.file_path)

        self.assertEqual("f1", f1.var_name)


    def test_pipeline_definition_sanity(self):

        dp = DryPipeDsl()

        def definition_with_key_collision():
            yield dp.task(key="k1")
            yield dp.task(key="k1")

        self.assertRaises(ValidationError, lambda: DryPipe.create_pipeline(definition_with_key_collision).create_pipeline_instance())

        def returns_no_iterable():
            return 2

        self.assertRaises(ValidationError, lambda: DryPipe.create_pipeline(returns_no_iterable).create_pipeline_instance())


        def returns_non_task():
            yield 234

        self.assertRaises(ValidationError, lambda: DryPipe.create_pipeline(returns_non_task).create_pipeline_instance())


class NonTrivialPipelineTests(unittest.TestCase):

    def test_steps_pipeline_01(self):

        d = TestSandboxDir(self)

        pipeline = d.pipeline_instance_from_generator(simple_static_pipeline)

        blast, report_task, python_much_fancier_report, python_much_much_fancier_report = pipeline.tasks

        upstream_deps_list = list(python_much_fancier_report.upstream_deps_iterator())

        (blast_ref, empty_file_deps, blast_ref_var_deps),\
        (report_task_ref, report_task_file_deps, report_task_var_deps) = upstream_deps_list

        self.assertEqual(blast, blast_ref)
        self.assertEqual(report_task, report_task_ref)

        self.assertEqual(len(empty_file_deps), 0)
        self.assertEqual(len(blast_ref_var_deps), 2)

        v1, v2 = blast_ref_var_deps

        self.assertEqual(v1.var_name_in_consuming_task, "v1")
        self.assertEqual(v2.var_name_in_consuming_task, "v2")

        self.assertEqual(len(report_task_file_deps), 1)
        self.assertEqual(len(report_task_var_deps), 3)

        fancy_report = report_task_file_deps[0]
        self.assertEqual(fancy_report.var_name_in_consuming_task, "fancy_report")

        self.assertDictEqual(
            vars(fancy_report.produced_file),
            {
                "file_path": 'fancy_report.txt',
                "glob_pattern": None,
                "manage_signature": None,
                'is_dummy': False,
                "producing_task": report_task,
                "var_name": 'fancy_report'
            }
        )

        self.assertEqual(
            ["vs1", "vs2", "vx"],
            list(map(lambda v: v.var_name_in_consuming_task, report_task_var_deps))
        )

        self.assertEqual(
            ['s1', 's2', 'x'],
            list(map(lambda v: v.output_var.name, report_task_var_deps))
        )

        for v in report_task_var_deps:
            self.assertIsNotNone(v.output_var.producing_task)

        self.assertEqual(
            [False, True, False],
            list(map(lambda v: v.output_var.may_be_none, report_task_var_deps))
        )

        self.assertEqual(
            [str, str, int],
            list(map(lambda v: v.output_var.type, report_task_var_deps))
        )

    def test_pipeline_graph(self):

        pipeline_instance = TestSandboxDir(self).pipeline_instance_from_generator(simple_static_pipeline)

        tasks, deps = pipeline_instance.summarized_dependency_graph()


class WithManyConfigCombinationsTests(unittest.TestCase):

    def validate_task_control(self, pipeline):
        run_and_validate_pipeline_execution(pipeline, self)


class NonTrivialPipelineLocalContainerlessTests(WithManyConfigCombinationsTests):

    def test_non_trivial_local_containerless(self):
        d = TestSandboxDir(self)

        pi = d.pipeline_instance_from_generator(simple_static_pipeline)

        run_and_validate_pipeline_execution(pi, self)

    def test_task_query_language(self):

        d_dep1 = TestSandboxDir(self, "EXT_NonTrivialPipelineLocalContainerlessTests.test_non_trivial_local_containerless")
        d_dep1.delete_and_recreate_sandbox()
        d_dep1.pipeline_instance_from_generator(
            simple_static_pipeline,
            completed=True
        )

        pid = d_dep1.sandbox_dir

        pipeline_instance = DryPipe.load_pipeline(pid)

        self.assertEqual(len(pipeline_instance.query("*").tasks), 4)

        self.assertIsNone(
            pipeline_instance.query("no match").single_or_none(),
            "query should have returned none"
        )

        with self.assertRaises(Exception) as _:
            pipeline_instance.query("*").single_or_none()

        with self.assertRaises(Exception) as _:
            pipeline_instance.query("*").single()

        with self.assertRaises(Exception) as _:
            pipeline_instance.query("no match").single()

        self.assertEqual(len([matcher for matcher in pipeline_instance.query("*")]), 1)

        self.assertEqual(len([matcher for matcher in pipeline_instance.query("no match")]), 0)

        def query_single(pattern):
            return pipeline_instance.query(pattern).single()

        def check_out_file(task, outfile, f):

            v = outfile
            if not os.path.exists(v):
                raise Exception(f"file should exist {v}")

            self.assertEqual(os.path.abspath(v), f"{pid}/output/{task.key}/{f}")

        python_much_fancier_report_1 = query_single("python_much_fancier_report.1")
        check_out_file(
            python_much_fancier_report_1, python_much_fancier_report_1.outputs.much_fancier_report, "fancier_report1.txt"
        )

        self.assertEqual(python_much_fancier_report_1.inputs.fancy_int, 1)


        blast_1 = query_single("blast.1")

        self.assertEqual(f"{pid}/human.fasta", blast_1.inputs.subject)

        check_out_file(blast_1, blast_1.outputs.blast_out, "human_chimp_blast.tsv")
        self.assertEqual(int(blast_1.outputs.v1), 1111)
        self.assertEqual(float(blast_1.outputs.v2), 3.14)

        report = query_single("report")

        self.assertEqual(int(report.outputs.x), 9876)
        self.assertEqual(str(report.outputs.s1), 'abc')

        self.assertEqual(f"s-{report.outputs.x}", f"s-9876")
        self.assertEqual(f"s-{report.outputs.s1}", f"s-abc")

        os.environ["test_non_trivial_local_containerless"] = pid

        pipeline_instance = DryPipe.load_pipeline("$test_non_trivial_local_containerless")

        blast_1 = [t for t in pipeline_instance.query("blast.1").tasks][0]

        self.assertEqual(int(blast_1.outputs.v1), 1111)


class NonTrivialPipelineLocalWithSingularityContainerTests(WithManyConfigCombinationsTests):

    def test_non_trivial_local_with_singularity(self):

        d = TestSandboxDir(self)

        pi = DryPipe.create_pipeline(simple_static_pipeline).create_pipeline_instance(
            pipeline_instance_dir=d.sandbox_dir,
            containers_dir=test_containers_dir(),
            task_conf=TaskConf(
                executer_type="process",
                container="singularity-test-container.sif",
                #python_bin="/usr/bin/python3"
            )
        )

        copy_pre_existing_file_deps_from_code_dir(pi)

        run_and_validate_pipeline_execution(pi, self)


class NonTrivialPipelineSlurmContainerlessTests(WithManyConfigCombinationsTests):

    def test_non_trivial_slurm_containerless(self):

        d = TestSandboxDir(self)

        pi = DryPipe.create_pipeline(simple_static_pipeline).create_pipeline_instance(
            pipeline_instance_dir=d.sandbox_dir,
            task_conf=TaskConf(
                executer_type="slurm",
                slurm_account="def-xroucou_cpu",
                sbatch_options=[
                    "--time=0:5:00"
                ],
                python_bin="/nfs3_ib/ip29-ib/ip29/rodrigue_group/ibio/miniconda3/envs/drypipe/bin/python3",
                init_bash_command="export PYTHONPATH=/home/maxl/dev/drypipe"
            )
        )

        copy_pre_existing_file_deps_from_code_dir(pi)

        run_and_validate_pipeline_execution(pi, self)


class NonTrivialPipelineSlurmWithSingularityContainerTests(WithManyConfigCombinationsTests):

    def test_non_trivial_slurm_with_singularity(self):

        d = TestSandboxDir(self)

        pi = DryPipe.create_pipeline(simple_static_pipeline).create_pipeline_instance(
            pipeline_instance_dir=d.sandbox_dir,
            task_conf=TaskConf(
                executer_type="slurm",
                slurm_account="def-xroucou_cpu",
                sbatch_options=[
                    "--time=0:5:00"
                ],
                python_bin="/usr/bin/python3",
                container="singularity-test-container.sif"
            ),
            containers_dir="/home/maxl/dev/drypipe/tests/containers"
        )

        copy_pre_existing_file_deps_from_code_dir(pi)

        run_and_validate_pipeline_execution(pi, self)


def attempt_seqence(n, initial_sleep, sleep_interval_after, sleep_max, expire_msg):

    time.sleep(initial_sleep)

    for i in range(0, n):

        yield i+1

        time.sleep(sleep_interval_after)

    raise Exception(f"{n} attempts made, and failed: {expire_msg}")


def python_bin_containerless():
    return sys.executable
