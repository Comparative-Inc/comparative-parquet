#!/bin/bash
set -euf
set -o pipefail

which node-gyp

npx node-gyp configure --release
npx node-gyp build
