#!/bin/bash

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}";
LINUX_SCRIPT_DIR="$( dirname -- "$SCRIPT_PATH"; )";
SCRIPT_DIR="$( dirname -- "$LINUX_SCRIPT_DIR"; )";
PROJECT_ROOT="$( dirname -- "$SCRIPT_DIR"; )";
echo "$PROJECT_ROOT";
cd "$PROJECT_ROOT" || exit;

conda run -n ppe python -m src.main