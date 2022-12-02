

def dag_gen_for_set_e_test(dsl):
    yield dsl.task("t1").produces(r=dsl.var(str)).calls(
        """
        #!/usr/bin/bash
        set -e        
        false
        # should not get here
        export r="bad"
        """
    )()
