# stellar_history_sync
Some files may be missed in certain history archive, here is a tool to check integrity and fetch up files by comparing between archives


history archive type planned to support:
* aws s3 (s3://*)
* aliyun oss (oss://*)
* local file system (/*) (TODO)

## TODO:

* support history archive in local directory
* auto upload missing files
* check more beyond file path only (eg. file size, hash, ...)
* try to use credentials from default location 
* deeper intergrity check by parsing history.json for buckets
