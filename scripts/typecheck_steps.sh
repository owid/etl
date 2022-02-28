#!/bin/bash
#
#  typecheck_steps
#

all_step_dirs() {
    echo etl/steps/*/*/*/*/
}

only_dirs_with_python_files() {
    for dir in $(all_step_dirs); do
        if find $dir -name '*.py' -print -quit | grep -q . ; then
            echo $dir
        fi
    done
}

echo 'Typechecking steps...'
only_dirs_with_python_files | xargs -n 1 .venv/bin/mypy
