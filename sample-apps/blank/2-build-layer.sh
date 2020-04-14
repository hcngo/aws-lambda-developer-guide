#!/bin/bash
set -eo pipefail
mkdir -p lib/nodejs
rm -rf lib/nodejs/node_modules
npm install --production
mv node_modules lib/nodejs/