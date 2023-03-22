
from dry_pipe import DryPipe


@DryPipe.python_call()
def f3(x1, x2):

    print("f3")

    return {
        "x3": x1 + x2
    }


def dag_generator(dsl):
    yield dsl.task(
        key="t"
    ).produces(
        x1=dsl.var(int),
        x2=dsl.var(int),
        x3=dsl.var(int)
    ).calls("""
        #!/usr/bin/env bash        
        export x1=7
    """).calls("""
        #!/usr/bin/env bash                
        export x2=$(( $x1 * 2 ))    
    """).calls(f3)()


def validate(test_case, pipeline_instance):

    t = pipeline_instance.tasks["t"]

    test_case.assertTrue(t.get_state().is_completed())

    x1 = t.out.x1.fetch()
    x2 = t.out.x2.fetch()
    x3 = t.out.x3.fetch()

    test_case.assertEquals(x1, 7)
    test_case.assertEquals(x2, 14)
    test_case.assertEquals(x3, 21)
