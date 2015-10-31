#! /bin/bash

kernels=$(docker ps|awk -F' {2,}' '{ print $7; }'|tail -n+2|grep kernel.python)
if [ -n "$kernels" ]; then
    echo "Killing kernel containers..."
    echo "$kernels"|xargs docker kill
    echo "Removing kernel containers..."
    echo "$kernels"|xargs docker rm
else
    echo "No kernel containers found."
fi
