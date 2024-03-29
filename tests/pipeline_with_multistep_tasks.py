
from dry_pipe import DryPipe


def three_steps_pipeline_task_generator(dsl):

    three_phase_task = dsl.task(
        key="three_phase_task"
    ).produces(
        out_file=dsl.file("out_file.txt")
    ).calls("""
        #!/usr/bin/env bash
        
        if [[ "${CRASH_STEP_1}" ]]; then
          exit 1
        fi
        
        echo "s1" >> $out_file    
    """).calls("""
        #!/usr/bin/env bash
        
        if [[ "${CRASH_STEP_2}" ]]; then
          exit 1
        fi
        
        
        echo "s2" >> $out_file    
    """).calls("""
        #!/usr/bin/env bash
        
        if [[ "${CRASH_STEP_3}" ]]; then
          exit 1
        fi
        
        echo "s3" >> $out_file    
    """)()

    yield three_phase_task


def three_steps_pipeline_expected_output():
    return "s1\ns2\ns3\n"


@DryPipe.python_call()
def step2_in_python(out_file):
    with open(out_file, "a") as f:
        f.write("s2\n")

@DryPipe.python_call()
def step4_in_python(out_file):
    with open(out_file, "a") as f:
        f.write("s4\n")


def hybrid_bash_python_mutlistep_pipeline_task_generator(dsl):

    three_phase_task = dsl.task(
        key="three_phase_task"
    ).produces(
        out_file=dsl.file("out_file.txt")
    ).calls(
        """
            #!/usr/bin/env bash
            echo "zaz"
            
            if [[ "${CRASH_STEP_1}" ]]; then
              exit 1
            fi
            
            echo "s1" >> $out_file        
        """,
        container="singularity-test-container.sif"
    ).calls(
        step2_in_python,
        container="singularity-test-container.sif"
    ).calls("""
        #!/usr/bin/env bash
        
        if [[ "${CRASH_STEP_3}" ]]; then
          exit 1
        fi
        
        echo "s3" >> $out_file    
    """).calls(
        step4_in_python
    )()

    yield three_phase_task


def pipeline():
    return DryPipe.create_pipeline(three_steps_pipeline_task_generator)
