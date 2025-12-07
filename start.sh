#!/bin/bash

# Use uvicorn to run the FastAPI application.
# The app object is named 'app' and is located in the file 'main.py'.
# It listens on 0.0.0.0 (all network interfaces) and the port provided by Render.
exec uvicorn main:app --host 0.0.0.0 --port ${PORT}
