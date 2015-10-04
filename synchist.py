from optparse import OptionParser
import getpass
import json
import itertools

import boto3

s3res = boto3.resource('s3')

from oss.oss_api import *
from oss import oss_xml_handler

#class HistoryBucket(HistoryArchive):
class HistoryBucket:
    flat_components = ["bucket"]
    components = ["history",  "ledger",  "results",  "transactions"]
    headflag = ".well-known/stellar-history.json"
    head = {}

    def __init__(self, bucket, prefix):
        self.bucket = bucket
        self.prefix = prefix

    def __str__(self):
        return "%s %s %s" % (self.__class__.__name__, self.bucket, self.prefix)
    
    @staticmethod
    def segmentURI(PRODUCT_PREFIX, path):
        bucket = path
        if bucket.startswith(PRODUCT_PREFIX):
            bucket = bucket[len(PRODUCT_PREFIX):]
        else:
            return (None, path)
        tmp_list = bucket.split("/")
        if len(tmp_list) > 0:
            bucket = tmp_list[0]
        return bucket, path[(len(PRODUCT_PREFIX)+len(bucket)+len('/')):]

    def ledgerCheckpoints(self,lcl):
        return itertools.chain([0], xrange(63, lcl+1, 64))

    def encodeCheckPoint(self, component, ledger):
        hexstr = "%08x" % ledger
        return "%s/%s/%s/%s/%s-%s" % (component, hexstr[0:2], hexstr[2:4], hexstr[4:6], component, hexstr)

    def decodeCheckPoint(self, component, path):
        #"component/00/00/00/component-0000003f.*" => 63(0x3f)
        ledger = int(path[2*len(component)+11:2*len(component)+19], 16)
        if not path.startswith(self.encodeCheckPoint(component, ledger)):
            raise "%s not a %s checkpoint" % (path, component)
        else:
            return ledger

# class LocalFolder(HistoryArchive):
#     @staticmethod 
#     def parseMatch(uri):
#         return None
    
class S3Folder(HistoryBucket):

    def __init__(self, bulket, prefix):
        HistoryBucket.__init__(self, bulket, prefix)

    @staticmethod 
    def parseMatch(uri):
        S3_PREFIX = 's3://'
        (bucket, prefix) = HistoryBucket.segmentURI(S3_PREFIX, uri)
        if (bucket is None):
            return None
        else:
            return S3Folder(bucket, prefix)

#    def getLCL(self): 
#bucket = s3res.Bucket("pangu.mythology.strllar.org")
#files = bucket.objects.filter(Prefix="klm")
# for file in files:
#     print file

# print("done")

       
class OSSFolder(HistoryBucket):
    def __init__(self, bulket, prefix):
        HistoryBucket.__init__(self, bulket, prefix)
    
    @staticmethod 
    def parseMatch(uri):
        OSS_PREFIX = 'oss://'
        (bucket, prefix)  =  HistoryBucket.segmentURI(OSS_PREFIX, uri)
        if (bucket is None):
            return None
        else:
            return OSSFolder(bucket, prefix)

    def checkCredentials(self):
        ep = raw_input('Endpoint of OSS(eg. oss-cn-beijing.aliyuncs.com): ')
        keyid = raw_input('accessKeyId of OSS: ')
        keysecret = getpass.getpass('accessKeySecret of OSS: ')
        self.oss = OssAPI(ep, keyid, keysecret)
        
    def getLCL(self):
        res=self.oss.get_object(self.bucket, self.prefix + '/' + self.headflag)
        head = json.loads(res.read())
        self.head = head
        return self.head['currentLedger']

    def onePage(self, prefix, marker, pagesize):
        res = None
        if (marker == None):
            res = self.oss.list_bucket(self.bucket, prefix=prefix, maxkeys=pagesize)
        else:
            res = self.oss.list_bucket(self.bucket, prefix=prefix, marker=marker, maxkeys=pagesize)
        objects_xml=oss_xml_handler.GetBucketXml(res.read())
        if len(objects_xml.content_list) < pagesize:
            return None, objects_xml.content_list
        else:
            return objects_xml.content_list[-1].key, objects_xml.content_list
        
    def allPages(self, subdir, pagesize = 100):
        marker, page = self.onePage(self.prefix+'/'+subdir, None, pagesize)

        for obj in page:
            yield (obj.key, int(obj.size))
        
        while marker is not None:
            marker, page = self.onePage(self.prefix+'/'+subdir, marker, pagesize)
            for obj in page:
                yield (obj.key, int(obj.size))

    def takeOverview(self):
        for component in self.components:
            stored = set([self.decodeCheckPoint(component, one[0][len(self.prefix)+len('/'):]) for one in self.allPages(component, 1000)])
            print("missing %s: "%component, set(self.ledgerCheckpoints(self.getLCL())).difference(stored))
        
def prepareBucket(uri):
    parsed =  [x for x in [OSSFolder.parseMatch(uri), S3Folder.parseMatch(uri)] if x is not None]
    if (len(parsed) > 0):
        folder = parsed[0]
        print "checking %s" % folder
        folder.checkCredentials()
        print("Lastest Closed Ledger is %d" % folder.getLCL())
        return folder
    else:
        return None


if __name__ == "__main__":
    parser = OptionParser()
    (options, args) = parser.parse_args()

    if (len(args) == 1):
        one = prepareBucket(args[0])
        one.takeOverview()
    elif (len(args) == 2):
        source = prepareBucket(args[0])
        dest = prepareBucket(args[1])
        print "syncing from %s -> %s" % (source, dest)





