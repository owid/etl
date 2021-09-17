#
#  Makefile
#


include default.mk

SRC = etl tests

watch:
	poetry run watchmedo shell-command -c 'clear; make unittest' --recursive --drop .
