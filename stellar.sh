#!/bin/bash

depends_on() {
	$1 --version >/dev/null 2>&1 || { echo >&2 "$1 is required but it's not installed.  Aborting."; exit 1; }
}

depends_on "curl"
depends_on "jq"
depends_on "parallel"

former_checkpoints() {	
	local lcl=$(curl -s $1/.well-known/stellar-history.json|jq ".currentLedger")
	echo 0
	seq 63 64 `expr $lcl - 1`
}

remotepath() {
	#echo "/$2/$(sed 's/\(.\{2\}\)\(.\{2\}\)\(.\{2\}\).*/\1\/\2\/\3/' <<<$1)/$2-$1.$3"
	echo /$2/${1:0:2}/${1:2:2}/${1:4:2}/$2-$1.$3
}

curl_download() {
	local cate=${2%%" "*}
	local exte=${2##*" "}	
	local lclhex=$(printf "%08x" $1)
	local subpath=$(remotepath $lclhex $cate $exte)
	if [ ! -f $4$subpath ];
	then
		curl -s --create-dirs $3$subpath -o $4$subpath
	else
		echo "skip existing $4$subpath"
	fi
}

download_checkpoints() {
	export -f remotepath
	export -f curl_download
	parallel -j3000% -u --eta curl_download {1} {2} $1 $2 ::: $(former_checkpoints $1) ::: "history json" "ledger xdr.gz" "transactions xdr.gz"
}

case $1 in
	'ex')
		shift
		#TODO as recursive subroutine
		;;
	*)
		download_checkpoints $1 $2
		;;
esac
