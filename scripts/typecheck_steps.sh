#!/bin/bash
#
#  typecheck_steps
#

all_step_dirs() {
    echo etl/steps/*/*/*/*/
}

only_dirs_with_python_files() {
    for dir in $(all_step_dirs); do
        if find $dir -name '*.py' -not -path "*/.ipynb_checkpoints/*" -print -quit | grep -q . ; then
            echo $dir
        fi
    done
}

echo 'Typechecking steps...'
# run typechecks in parallel, we cannot run them all at once because mypy raises `Duplicate module named "shared"`
# error. There is no easy way around this (pyright does not have this problem)
only_dirs_with_python_files | xargs -n 1 -P 0 poetry run mypy
