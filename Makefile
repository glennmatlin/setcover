.PHONY: install test

default: test

install:
	pip install --upgrade .

test:
	PYTHONPATH=. python3 -m pytest tests
	#SPARK_HOME /opt/spark