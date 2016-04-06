#!/bin/bash

bin/run-common.sh

echo "$GIT_SHA" > static/revision.txt

exec gunicorn basket.wsgi:application \
              --bind "0.0.0.0:${PORT:-8000}" \
              --workers "${WSGI_NUM_WORKERS:-2}" \
              --log-level "${WSGI_LOG_LEVEL:-info}" \
              --error-logfile -
