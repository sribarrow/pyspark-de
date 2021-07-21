
## Directory Structure
.
-- ReadMe.md
-- csv
-- --- weather.20160201.csv
-- --- weather.20160301.csv
-- parr.py
-- pq
-- --- weather.parquet
-- requirements.txt
-- test_parr.py

## Reasoning for python modules

1. I made a choice between pandas and pyarrow. Since pandas uses pyarrow, and pyarrow is more efficient, I chose to use pyarrow over pandas.
2. I have used Pyspark, as its one of the requirements for the role and it gives the ability to query and apply aggregation like SQL.
3. Initially I thought I will not use class in this simple case as I am using 2 different modular functions. But I changed it to a class as it prevented the lag of the pyspark load while testing.
4. I have used the common test modules.
5. Since there is no specifics on how to display, I have chosen an interactive commandline option.
6. I have broken Rowgroup to 10000 rows each. Since this is a smaller data, I think there is no big win. But filtering would have been effective as less rows would have been scanned.

## Prerequisites
Requirements.txt has list of installations required

# Environment Variables to be set for Pyspark 
- ** Mine is a mac. This may vary for other environments ** 
export JAVA_HOME={Java 8 path}
export SPARK_HOME={PySpark installation path}
SBT_HOME=$SPARK_HOME/sbt
SCALA_HOME=$SPARK_HOME/scala-{version}
PYSPARK_PYTHON=python3
PYSPARK_DRIVER_PYTHON=python3
PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-{version}-src.zip:$PYTHONPATH
export PATH=$JAVA_HOME/bin:$SBT_HOME/bin:$SBT_HOME/lib:$SCALA_HOME/bin:$SCALA_HOME/lib:$PATH
export PATH=$JAVA_HOME/bin:$SPARK_HOME:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

## How to run the program

From Project Dir: Data Engineer Test/src

python3 parr.py

## How to test the program

pytest -vx test_parr.py 

## Test Results

$ pytest -vx test_parr.py 
==================================================== test session starts ==================================================================
platform darwin -- Python 3.9.6, pytest-6.2.4, py-1.10.0, pluggy-0.13.1 -- /usr/local/opt/python@3.9/bin/python3.9
cachedir: .pytest_cache
rootdir: /Users/sbmacpro/Exercism/python/Data Engineer Test/src
collected 7 items                                                                                                                                       

test_parr.py::PyDataFrameTest::test_import_novalue PASSED                                                                                      [ 14%]
test_parr.py::PyDataFrameTest::test_import_only_dest PASSED                                                                                      [ 28%]
test_parr.py::PyDataFrameTest::test_import_src_w_path PASSED                                                                                      [ 42%]
test_parr.py::PyDataFrameTest::test_import_src_wo_path PASSED                                                                                      [ 57%]
test_parr.py::PyDataFrameTest::test_read_from_parquet_with_ps_no_arg PASSED                                                                                      [ 71%]
test_parr.py::PyDataFrameTest::test_read_from_parquet_with_ps_w_correct_path PASSED                                                                                      [ 85%]
test_parr.py::PyDataFrameTest::test_read_from_parquet_with_ps_w_dir PASSED                                                                                      [100%]

================================================= 7 passed in 13.24s ===================================================================
