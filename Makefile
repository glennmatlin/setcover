.PHONY: install test

default: test

install:
	pip install --upgrade .

test:
	PYTHONPATH=. python3 -m pytest --log-cli-level=0 test
	#SPARK_HOME /opt/spark