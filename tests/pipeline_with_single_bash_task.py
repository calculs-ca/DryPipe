
def pipeline(dsl):
    yield dsl.task(
        key="multiply_x_by_y"
    ).consumes(
        x=dsl.val(3),
        y=dsl.val(5)
    ).produces(
        result=dsl.var(int)
    ).calls(
        """
        #!/usr/bin/bash        
        echo "Z1"
        echo "result=$((x * y))" > $__output_var_file
        echo "Z2"
        """
    )()


