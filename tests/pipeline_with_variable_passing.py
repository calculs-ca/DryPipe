

def pipeline(dsl):
    t1 = dsl.task(
        key="produce_a_var"
    ).produces(
        v=dsl.var(int)
    ).calls(
        """
        #!/usr/bin/bash                
        export v=1234        
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
        export static_result=abc
        export result=$((v * 2))
        """
    )()





