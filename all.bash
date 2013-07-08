#!/bin/bash
set -e

cmd=${*-test}

for d in ./*/; do
	if ls $d/*.go >/dev/null 2>/dev/null; then
		name=`basename $d`
		echo "---"
		echo "$name"
		echo ""
		( cd "$d"; go $cmd )
	fi
done
