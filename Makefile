.PHONY: default refactor mypy test


default: refactor

refactor:
	@yapf -r -i . 
	@isort . 
	@pycln -a .

mypy:
	@mypy .

test:
	pytest ./ttl/jmain.py

build:
	poetry build