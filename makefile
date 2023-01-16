SRC := $(wildcard *.py)
PYTHON := python

all: help


## help:		show this help
help:
	@sed -n 's/^##//p' Makefile

## check: 	format and lint the code.
check:
	yapf -i ${SRC}

## run:		run current task
run: DEBUG=1
run:
	@${PYTHON} current.py

## test:		run tests
test:
	@${PYTHON} -m unittest discover -s .

## deploy:	deploys the latest change to fly.io
deploy:
	flyctl deploy --local-only

## clean:	cleans garbage
clean:
	-rm -rf __pycache__
	-rm -rf .mypy_cache

.PHONY: help check run test deploy clean
