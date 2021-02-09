.PHONY: install test

default: test

install:
	pip install --upgrade .

test:
	PYTHONPATH=. python3 -m --log-cli-level=10 pytest tests
	#SPARK_HOME /opt/spark