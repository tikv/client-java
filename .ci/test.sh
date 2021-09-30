#!/bin/bash

set -x
set -euo pipefail

export RAWKV_PD_ADDRESSES=127.0.0.1:2379
export TXNKV_PD_ADDRESSES=127.0.0.1:3379
mvn clean test