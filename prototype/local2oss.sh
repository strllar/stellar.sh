#!/usr/bin/env bash

osscmd -H oss-cn-beijing.aliyuncs.com uploadfromdir --skip_dir=.well-known/ history/local oss://stellar/xlm
