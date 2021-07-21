#!/bin/bash
set -euf
set -o pipefail

npm run configure
npm run build
