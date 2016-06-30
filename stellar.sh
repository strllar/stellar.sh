#!/bin/bash

shopt -s extglob

depends_on() {
	$1 --version >/dev/null 2>&1 || { echo >&2 "$1 is required but it's not installed.  Aborting."; exit 1; }
}

depends_on "curl"
depends_on "jq"
depends_on "parallel"
depends_on "gzip"

# Usage info
show_help() {
	cat <<EOF
Usages:
  1)  ${0##*/} hist [options] ledger/txs/state/scp/bucket-??
    -s SOURCE   source archive (eg. 'http://history.stellar.org/prd/core-live/core_live_001/')
    -d DESTDIR  local dir to store archive (eg. 'archive')
EOF
}


remotepath() {
	#echo "$2/$(sed 's/\(.\{2\}\)\(.\{2\}\)\(.\{2\}\).*/\1\/\2\/\3/' <<<$1)/$2-$1.$3"
	echo $2/${1:0:2}/${1:2:2}/${1:4:2}/$2-$1.$3
}

former_checkpoints() {
	echo "enumerating checkpints in range $1-$2 ledgers" >&2
	seq $1 64 `expr $2 - 1`
}

all_checkpoints() {
	former_checkpoints 63 $(curl -s $1/.well-known/stellar-history.json|jq ".currentLedger")
}

checkpoint_filename() {
	local cate=${2%%" "*}
	local exte=${2##*" "}	
	local lclhex=$(printf "%08x" $1)
	echo $(remotepath $lclhex $cate $exte)
}

curl_download() {
    local subpath=$(checkpoint_filename $1 $2)
	local valid=true
	case exte in
		*gz)
			if ! gzip -t $4/$subpath; then
				valid=false
			fi
		;;
		*json)
			if ! jq ".currentLedger|empty" $4/$subpath; then
				valid=false
			fi
			;;
	esac
		
	if [ $valid = true ];
	then
		echo "skip existing $4/$subpath"
	else
		#todo enable $? back forward
		echo "curl -s --create-dirs $3/$subpath -o $4/$subpath"
	fi
}

awk_checkpoint_filenames='
BEGIN{lclhex="";}
function remotepath(h,c,e) {
  return gensub(/^(..)(..)(..).*/, ENVIRON["LOCAL_HISTORY"] "/" c "/\\1/\\2/\\3/" c "-" "\\0." e, "g", h);
}
{
lclhex=sprintf("%08x",$1);
print(remotepath(lclhex, "history", "json"));
print(remotepath(lclhex, "ledger", "xdr.gz"));
print(remotepath(lclhex, "transactions", "xdr.gz"));
}'

filtered_checkpoints() {	
	export -f remotepath
	export -f checkpoint_filename
	export LOCAL_HISTORY=$2
	lstfile=$(mktemp --suffix .lst stellar.sh_XXXXXX)
	
	#generating the full list, touch the missing file for following 'find'
	all_checkpoints $1|awk "$awk_checkpoint_filenames"|tee $lstfile | parallel -m -u "mkdir -p {//} && touch {}"
	#check out for the corrupted files
	parallel --eta -m -a $lstfile -I%% 'find %% \( -size 0 -o \( -name *.gz \! -exec gzip -t {} \; \) -o \( -name *.json \! -exec sh -c "exec cat {}|jq \".currentLedger\" >/dev/null" \; \) \) -print'
	
	unlink $lstfile
}

download_subset() {
	export -f remotepath
	export -f curl_download
	
	parallel  -u --eta curl_download $((16#$1)) {1} $source_hist $dest_hist ::: "history json" "ledger xdr.gz" "transactions xdr.gz"
}

download_checkpoints() {
	export -f remotepath
	export -f curl_download
	
	all_checkpoints $1 | parallel -j3000% -u --eta curl_download {1} {2} $1 $2 :::: - ::: "history json" "ledger xdr.gz" "transactions xdr.gz"
}

case $1 in
	"hist")
		shift
		while getopts "s:d:" opt; do
			case "$opt" in
				s)
					source_hist=${OPTARG%/}
					;;
				d)
					dest_hist=${OPTARG%/}
					;;
				*)
					show_help >&2
					exit 1
					;;
			esac
		done
		shift "$((OPTIND-1))"
		
		if [[  x"${source_hist}" = x || x"${dest_hist}" = x ]];
		then echo >&2 "source and dest must be specified"; exit 1;
		fi

		case $1 in
			"sync")
				download_checkpoints $source_hist $dest_hist
				#TODO: autosync with advancing ledger
			;;
			@([0-9a-f])?([0-9a-f])?([0-9a-f])?([0-9a-f])?([0-9a-f])?([0-9a-f])?([0-9a-f])\
?([0-9a-f]))
			download_subset $1
			;;
			*)
				echo >&2 "Unkown file type or format!"
				exit 1;
			esac
								
		;;
	"acct")
		echo "todo"
		;;
	"play")
		shift
		filtered_checkpoints $1 $2
		exit $?
		former_checkpoints $1
		;;
	*)
		show_help
		exit 0
		;;
		
esac
