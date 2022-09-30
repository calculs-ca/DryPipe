

def pipeline(dsl):
    t1 = dsl.task(
        key="produce_a_var"
    ).produces(
        v=dsl.var(int)
    ).calls(
        """
        #!/usr/bin/bash        
        echo "v=1234" > $__output_var_file        
        """
    )()

    yield t1

    yield dsl.task(
        key="consume_and_produce_a_var"
    ).consumes(
        v=t1.out.v
    ).produces(
        result=dsl.var(int)
    ).calls(
        """
        #!/usr/bin/bash
        echo "static_result=abc" > $__output_var_file        
        echo "result=$((v * 2))" > $__output_var_file
        """
    )()





