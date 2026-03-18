#!/bin/bash
gunicorn -k uvicorn.workers.UvicornWorker api:app --workers 2 --bind 0.0.0.0:${PORT:-10000} --timeout 60 --graceful-timeout 30 --keep-alive 5
