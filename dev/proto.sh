#!/usr/bin/env bash
#
#   Copyright 2017 PingCAP, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

kvproto_hash=58f2ac94aa38f49676dd628fbcc1d669a77a62ac
raft_rs_hash=b9891b673573fad77ebcf9bbe0969cf945841926
tipb_hash=c4d518eb1d60c21f05b028b36729e64610346dac

kvproto_dir="kvproto"
raft_rs_dir="raft-rs"
tipb_dir="tipb"

CURRENT_DIR=$(pwd)
TIKV_CLIENT_HOME="$(
	cd "$(dirname "$0")"/.. || exit
	pwd
)"
cd "$TIKV_CLIENT_HOME" || exit

if [ -d "$kvproto_dir" ]; then
	git -C ${kvproto_dir} fetch -p
	git pull --all
else
	git clone https://github.com/pingcap/kvproto ${kvproto_dir}
fi
git -C ${kvproto_dir} checkout ${kvproto_hash}

if [ -d "$raft_rs_dir" ]; then
	git -C ${raft_rs_dir} fetch -p
	git pull --all
else
	git clone https://github.com/pingcap/raft-rs ${raft_rs_dir}
fi
git -C ${raft_rs_dir} checkout ${raft_rs_hash}

if [ -d "$tipb_dir" ]; then
	git -C ${tipb_dir} fetch -p
	git pull --all
else
	git clone https://github.com/pingcap/tipb ${tipb_dir}
fi
git -C ${tipb_dir} checkout ${tipb_hash}

cd "$CURRENT_DIR" || exit
