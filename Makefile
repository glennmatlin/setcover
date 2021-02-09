.PHONY: install test

default: test

install:
	pip install --upgrade .

test:
	PYTHONPATH=. python3 -m pytest --log-cli-level=10 tests
	#SPARK_HOME /opt/spark