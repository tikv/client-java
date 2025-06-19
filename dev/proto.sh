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

proto_dir="proto"

if [ -d $proto_dir ]; then
	rm -r $proto_dir
fi

repos=("https://github.com/pingcap/kvproto" "https://github.com/pingcap/raft-rs" "https://github.com/pingcap/tipb")
commits=(a0e3fbb1eeee6b8e04e2bec6d1ca65dcd9172662 7c21f8d12f2043c43004fca48861568db7c76bd9 2fb8289108131ccd5faf86d3e92fb05d4ff3b326)

for i in "${!repos[@]}"; do 
	repo_name=$(basename ${repos[$i]})
	git_command="git -C $repo_name"

	if [ -d "$repo_name" ]; then
		$git_command checkout `basename $($git_command symbolic-ref --short refs/remotes/origin/HEAD)`
		$git_command fetch --all
		$git_command pull --all
	else
		git clone ${repos[$i]} $repo_name
		$git_command fetch -p
	fi

	$git_command checkout ${commits[$i]}
done
