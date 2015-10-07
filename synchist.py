from optparse import OptionParser
import getpass
import json
from tempfile import mkdtemp

from boto3.session import Session

from oss.oss_api import *
from oss import oss_xml_handler

import os, errno
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise
        
class HistoryArchive:
    flat_components = ["bucket"]
    components = ["history",  "ledger",  "results",  "transactions"]
    headflag = ".well-known/stellar-history.json"
    head = None

    def listEntries(self, component): pass
    def getRawEntry(self, path): pass


    def saveEntryToFile(self, component, uniqid, topdir):
        subpath = self.resolveComponent(component, uniqid)
        localpath = os.path.join(topdir, subpath)
        localdir = os.path.dirname(localpath)
        mkdir_p(localdir)
        f = file(localpath, "wb")
        f.write(self.getRawEntry(subpath))
        f.close()
        return (subpath, localpath)

    # def putEntry(self, component, uniqid): pass
    # def putLocalizedEntry(self, localpath): pass

    def getLCL(self, update=False):
        if (self.head is None or update):
            head = json.loads(self.getRawEntry(self.headflag))
            self.head = head            
        return self.head['currentLedger']

    def resolveComponent(self, component, uniqid):
        hexstr = uniqid
        ext = ".xdr.gz"
        if (component == "history"):
            ext = ".json"            
        return "%s/%s/%s/%s/%s-%s%s" % (component, hexstr[0:2], hexstr[2:4], hexstr[4:6], component, hexstr, ext)
    
    def ledgerCheckpoints(self,lcl):
        return xrange(63, lcl+1, 64)

    def _encodeCheckPoint(self, component, ledger):
        return self.resolveComponent(component, "%08x" % ledger)

    def decodeCheckPoint(self, component, path):
        #"component/00/00/00/component-0000003f.*" => 63(0x3f)
        ledger = int(path[2*len(component)+11:2*len(component)+19], 16)
        if (path == self._encodeCheckPoint(component, ledger)):
            return ledger
        else:
            raise "%s not a %s checkpoint" % (path, component)

    def decodeFlat(self, component, path):
        #"component/de/ad/be/component-deadbeef....*" => deadbeef...
        hash = path[2*len(component)+11:-len(".xdr.gz")]
        if (path == self.resolveComponent(component, hash)):
            return hash
        else:
            raise "%s not a %s path" % (path, component)

        
    def takeOverview(self):
        for component in self.components:
            stored = set([self.decodeCheckPoint(component, one[0]) for one in self.listEntries(component)])
            print "missing %s: %s" % (component, sorted(list(set(self.ledgerCheckpoints(self.getLCL())).difference(stored))))

    
class HistoryBucket(HistoryArchive):
    def __init__(self, bucket, prefix):
        self.bucket = bucket
        self.prefix = prefix

    def __str__(self):
        return "%s(%s/%s)" % (self.__class__.__name__, self.bucket, self.prefix)
    
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
        
        return bucket, path[(len(PRODUCT_PREFIX)+len(bucket)+len('/')):].rstrip('/')

    def listEntries(self, component):
        return map(lambda x: (x[0][len(self.prefix)+len('/'):], x[1]), self.allPages(component, 1000))
    
class LocalFolder(HistoryArchive): pass
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

    def checkCredentials(self):
        region = raw_input('Region for AWS S3(eg. us-east-1): ').strip()        
        keyid = raw_input('Access Key ID for AWS S3: ').strip()
        keysecret = getpass.getpass('Access Key Secret for AWS S3: ').strip()
        sess = Session(aws_access_key_id=keyid,
                         aws_secret_access_key=keysecret,
                       region_name=region)
        self.s3 = sess.resource('s3')
        self.s3bucket = self.s3.Bucket(self.bucket)

    def onePage(self, prefix, marker, pagesize):
        res = None
        if (marker == None):
            res = list(self.s3bucket.objects.filter(Prefix=prefix, MaxKeys=pagesize))
        else:
            res = list(self.s3bucket.objects.filter(Prefix=prefix, Marker=marker, MaxKeys=pagesize))

        if len(res) < pagesize:
            return None, res
        else:
            return res[-1].key, res

    def allPages(self, subdir, pagesize = 100):
        marker, page = self.onePage(self.prefix+'/'+subdir, None, pagesize)

        for obj in page:
            yield (obj.key, int(obj.size))
        
        while marker is not None:
            marker, page = self.onePage(self.prefix+'/'+subdir, marker, pagesize)
            for obj in page:
                yield (obj.key, int(obj.size))

    def getRawEntry(self, path):
        res=self.s3.Object(self.bucket, self.prefix + '/' + path).get()
        return res['Body'].read()

       
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
        ep = raw_input('Endpoint of OSS(eg. oss-cn-beijing.aliyuncs.com): ').strip()
        keyid = raw_input('accessKeyId of OSS: ').strip()
        keysecret = getpass.getpass('accessKeySecret of OSS: ').strip()
        self.oss = OssAPI(ep, keyid, keysecret)
        
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

    def getRawEntry(self, path):
        res=self.oss.get_object(self.bucket, self.prefix + '/' + path)
        return res.read()

def prepareBucket(uri):
    parsed =  [x for x in [OSSFolder.parseMatch(uri), S3Folder.parseMatch(uri)] if x is not None]
    if (len(parsed) > 0):
        folder = parsed[0]
        print "checking %s" % folder
        folder.checkCredentials()
        print("Last Closed Ledger is %d" % folder.getLCL())
        return folder
    else:
        return None

def diffBuckets(src, dest):
    diffs = []
    for component in src.components:
        srcset = set([src.decodeCheckPoint(component, one[0]) for one in src.listEntries(component)])
        destset = set([dest.decodeCheckPoint(component, one[0]) for one in dest.listEntries(component)])
        if (len(set(src.ledgerCheckpoints(dest.getLCL())).difference(srcset.union(destset))) == 0):
            #print "need copy %s @%s"  % (component, sorted(list(srcset.difference(destset))))
            for x in srcset.difference(destset):
                diffs.append((component, "%08x" % x))
        else:
            raise "WARNING: even combination of archives is not complete, get a new archive!"

    for component in src.flat_components:
        srcset = set([src.decodeFlat(component, one[0]) for one in src.listEntries(component)])
        destset = set([dest.decodeFlat(component, one[0]) for one in dest.listEntries(component)])
        #print "need copy %s @%s"  % (component, sorted(list(srcset.difference(destset))))
        for y in srcset.difference(destset):
            diffs.append((component, y))

    return diffs
    
if __name__ == "__main__":
    parser = OptionParser()
    (options, args) = parser.parse_args()

    if (len(args) == 1):
        one = prepareBucket(args[0])
        print "Calc missing files in {}".format(one)
        one.takeOverview()
        
    elif (len(args) == 2):
        source = prepareBucket(args[0])
        dest = prepareBucket(args[1])
        print "Calc missing files in {1} from reference {0}".format(source, dest)
        diffs = diffBuckets(source, dest)

        tempd = mkdtemp(prefix="stellar-history-diff-")
        print "Fetching files from {} to {} ..".format(source, tempd)
        for xyz in diffs:        
            res = source.saveEntryToFile(xyz[0], xyz[1], tempd)
            print "saved {}".format(res[0])

    else:
        print "to check a history archive:"
        print "<archive>"
        print "to collect missing file for a corrupt history archive:"
        print "<good archive> <corrupt archive>"



