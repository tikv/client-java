#!/bin/sh
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

CURRENT_DIR=`pwd`
TIKV_CLIENT_HOME="$(cd "`dirname "$0"`"/..; pwd)"
cd $TIKV_CLIENT_HOME

kvproto_hash=6ed99a08e262d8a32d6355dcba91cf99cb92074a

raft_rs_hash=b9891b673573fad77ebcf9bbe0969cf945841926

tipb_hash=c4d518eb1d60c21f05b028b36729e64610346dac

<<<<<<< HEAD
if [ -d "kvproto" ]; then
	cd kvproto; git fetch -p; git checkout ${kvproto_hash}; cd ..
=======
if [ -d "$kvproto_dir" ]; then
	git -C ${kvproto_dir} fetch -p
	git pull --all
>>>>>>> 7a123a07e... [close #591] update all the branch of proto while precompiling to avoid commit miss (#592)
else
	git clone https://github.com/pingcap/kvproto; cd kvproto; git checkout ${kvproto_hash}; cd ..
fi

<<<<<<< HEAD
if [ -d "raft-rs" ]; then
	cd raft-rs; git fetch -p; git checkout ${raft_rs_hash}; cd ..
=======
if [ -d "$raft_rs_dir" ]; then
	git -C ${raft_rs_dir} fetch -p
	git pull --all
>>>>>>> 7a123a07e... [close #591] update all the branch of proto while precompiling to avoid commit miss (#592)
else
	git clone https://github.com/pingcap/raft-rs; cd raft-rs; git checkout ${raft_rs_hash}; cd ..
fi

<<<<<<< HEAD
if [ -d "tipb" ]; then
	cd tipb; git fetch -p; git checkout ${tipb_hash}; cd ..
=======
if [ -d "$tipb_dir" ]; then
	git -C ${tipb_dir} fetch -p
	git pull --all
>>>>>>> 7a123a07e... [close #591] update all the branch of proto while precompiling to avoid commit miss (#592)
else
	git clone https://github.com/pingcap/tipb; cd tipb; git checkout ${tipb_hash}; cd ..
fi

cd $CURRENT_DIR
