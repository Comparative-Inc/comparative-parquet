#!/bin/bash
set -euf
set -o pipefail

export __dirname="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

npx node-gyp configure --release -- -f gyp.generator.compile_commands_json.py
mv ${__dirname}/../Release/compile_commands.json ${__dirname}/..
rmdir ${__dirname}/../Release
