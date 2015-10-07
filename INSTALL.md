```bash
$virtualenv --no-site-packages -p `which python` run
$cd run
$mkdir oss && cd oss
$curl -O http://strllar.oss-cn-beijing.aliyuncs.com/downloads/OSS_Python_API_20150909.zip
$unzip OSS_Python_API_20150909.zip
$../bin/python setup.py install
$cd ..
$bin/pip install pip boto3
```
