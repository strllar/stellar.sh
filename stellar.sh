#!/bin/bash

#Copyleft 2016 by Strllar Lab (lab_AT_strllar_DOT_org)

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
    Shell Script Utilities for Stellar Network
Usages:
  1)  For history archive related

   ${0##*/} archive [-s] [-t] -r <ref_hist> [local_hist]
     
    -s            keep local_hist autoSynced with polling
    -t            Test mode. output filename without acturly fetching
    -r <ref_hist> ref_hist is remote Reference archive (eg. 'http://history.stellar.org/prd/core-live/core_live_001/')
                  predefined abbrevs supported: sdf1 sdf2 sdf3
                  decimal or hex number of ledger is also supported. implicity enable test mode and disable autosync
    [local_hist]  the path of local archive, if omited, monitor latest ledger number of ref_hist

  2)  For account related (TODO)

   ${0##*/} newacct

   TBD
EOF
}

former_checkpoints() {
	echo "enumerating checkpoints before ledger $1..." >&2
	seq ${2:-63} 64 $1
}

curl_download() {
	if [ "$4" = true ];
	then
		echo $1
	else
		curl -sf -m 30 --create-dirs $2/$1 -o $3/$1
	fi
}

resolve_ref() {
	local url=$1
	case $1 in
		sdf1)
			url="http://history.stellar.org/prd/core-live/core_live_001/"
			;;
		sdf2)
			url="http://history.stellar.org/prd/core-live/core_live_002/"
			;;
		sdf3)
			url="http://history.stellar.org/prd/core-live/core_live_003/"
			;;
		#learn more from http://wiki.bash-hackers.org/syntax/pattern
		0[[:xdigit:]][[:xdigit:]][[:xdigit:]][[:xdigit:]][[:xdigit:]][[:xdigit:]][[:xdigit:]])
			echo $((16#$1))
			return 0
			;;
		@(+([[:digit:]]))) echo $1; return 0; ;;
	esac
    url=${url%/}

    curl -sf $url/.well-known/stellar-history.json|jq ".currentLedger" && echo $url
}

awk_checkpoint_filenames='
BEGIN{lclhex="";}
function remotepath(h,c,e) {
  return gensub(/^(..)(..)(..).*/, c "/\\1/\\2/\\3/" c "-" "\\0." e, "g", h);
}
{
lclhex=sprintf("%08x",$1);
print(remotepath(lclhex, "history", "json"));
print(remotepath(lclhex, "ledger", "xdr.gz"));
print(remotepath(lclhex, "transactions", "xdr.gz"));
}'

filtered_checkpoints() {	

	lstfile=$(pwd)/$(mktemp --suffix .lst stellar.sh_XXXXXX)
	
	#generating the full list, touch the missing file for following 'find'
	former_checkpoints $1|awk "$awk_checkpoint_filenames"|tee $lstfile | (cd $2; parallel -m -u "mkdir -p {//} && touch {}")

	#check out for the corrupted files
	(cd $2; parallel -m -a $lstfile -I%% 'find %% \( -size 0 -o \( -name *.gz \! -exec gzip -t {} \; \) -o \( -name *.json \! -exec sh -c "exec cat {}|jq \".currentLedger\" >/dev/null" \; \) \) -print')
	
	unlink $lstfile
}

do_archive() {
	local local_dir=${local_hist%/}
	read ledger_seq resolved_ref_url <<<$(resolve_ref $ref_hist)

	if [ -z "$resolved_ref_url" ];
	then
		echo "entering test mode since ref_hist is number" >&2
		test_mode=true;
	fi

	if [ -n "$local_dir" ];
	then
		export -f curl_download
		filtered_checkpoints $ledger_seq $local_dir | parallel -j3000% -u --retries 1 --eta curl_download {} \"$resolved_ref_url\" \"$local_dir\" \"$test_mode\"
	else
		echo "got last checkpoint at ledger "$ledger_seq
	fi

	if [ "$autosync" = true ];
	then
		while read lcl _url_ <<<$(resolve_ref $ref_hist);
		do			
			if [ $lcl -gt $ledger_seq ];			   
			then
				echo "got a new checkpoint at ledger $lcl"
				if [ -n "$local_dir" ];
				then
					former_checkpoints "$lcl" "$(($ledger_seq+64))"|awk "$awk_checkpoint_filenames"|parallel -u --eta curl_download {} \"$resolved_ref_url\" \"$local_dir\" \"$test_mode\"
				fi
				ledger_seq=$lcl
				sleep $((5*63));
			else				
				sleep 5;
			fi
		done
	fi
}

case $1 in
	"archive")
		shift
		while getopts "str:" opt; do
			case "$opt" in
				s)
					autosync=true
					;;
				t)
					test_mode=true
					;;
				r)
					ref_hist=$OPTARG
					;;
				*)
					show_help >&2
					exit 1
					;;
			esac
		done
		shift "$((OPTIND-1))"
		
		if [ -z "$ref_hist" ];
		then echo >&2 "reference archive must be specified"; exit 1;
		fi

		local_hist=$1

		do_archive
								
		;;
	"newacct")
		echo "todo"
		;;
	"play")
		shift
		resolve_ref $1
		exit $?
		filtered_checkpoints $1 $2
		former_checkpoints $1
		;;
	*)
		show_help
		exit 0
		;;
		
esac
