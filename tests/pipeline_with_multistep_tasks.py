
from dry_pipe import DryPipe, cli


def three_steps_pipeline(dsl):

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


@DryPipe.python_task()
def step2_in_python(out_file):
    with open(out_file, "a") as f:
        f.write("s2\n")

@DryPipe.python_task()
def step4_in_python(out_file):
    with open(out_file, "a") as f:
        f.write("s4\n")


def hybrid_bash_python_mutlistep_pipeline(dsl):

    three_phase_task = dsl.task(
        key="three_phase_task"
    ).produces(
        out_file=dsl.file("out_file.txt")
    ).calls(
        """
            #!/usr/bin/env bash
            
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
    return DryPipe.create_pipeline(three_steps_pipeline)
