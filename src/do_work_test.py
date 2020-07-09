from do_work import do_work, execute_spark_load
from pyspark import SparkContext

sc = SparkContext(master="local[2]")

def test_do_work():
    test_input = {}
    
    result = do_work(test_input)

    assert result['delay']

def test_execute_spark():
    result = execute_spark_load(sc, [dict()])

    assert len(result) > 0

def test_execute_spark2():
    result = execute_spark_load(sc, [dict()])

    assert len(result) > 0