.PHONY: install tests

default: tests

install:
	pip install --upgrade .

tests:
	PYTHONPATH=. python3 -m pytest --log-cli-level=0 tests
	#SPARK_HOME /opt/spark