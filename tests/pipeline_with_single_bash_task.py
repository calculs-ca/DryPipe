
def pipeline(dsl, task_conf=None):
    yield dsl.task(
        key="multiply_x_by_y",
        task_conf=task_conf
    ).consumes(
        x=dsl.val(3),
        y=dsl.val(5)
    ).produces(
        result=dsl.var(int)
    ).calls(
        """
        #!/usr/bin/bash        
        echo "Z1"        
        export result=$((x * y))
        echo "--->$result"
        echo "Z2"
        """
    )()


