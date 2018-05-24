#encode=utf-8
import os, sys
path = os.path.abspath(__file__).replace('\\', '/')
absdir = path[0:path.rfind('/')]
sys.path.append(absdir[0:absdir.rfind('/',0,len(absdir))])
import unittest
from collections import deque
from src.worker.storage import Storage
from src.common.sdk.s3client import S3Client

class TestStorage(unittest.TestCase):
    def setUp(self):
        client = S3Client('0DFA5B10A1B8EE0107CAAACD465EA7D4', '2DDFAA55EFEE553114929380EBD6CF88',\
                          'http://s3.cn-north-1.jcloudcs.com')
        bucketName = 'eric'
        prefix = 'storagetest/'
        cacheCapicity = 10
        nodeSize = 50
        self.storage = Storage(client, bucketName, prefix, cacheCapicity, nodeSize)

    def tearDown(self):
        pass

    def test_putOneEleShouldPass(self):
        self.assertTrue(self.storage.clear())

        mylist = list()
        mylist.append('1\t1')
        self.storage.put(mylist)
        self.assertEqual(self.storage.get('1'), '1')

    def test_putTwoEleShouldPass(self):
        self.assertTrue(self.storage.clear())

        mylist = list()
        mylist.append('2\t2')
        mylist.append('1\t1')

        self.storage.put(mylist)
        self.assertEqual(self.storage.get('2'), '2')
        self.assertEqual(self.storage.get('1'), '1')

    def test_putBinarySearchTreeNotEmptyShouldPass(self):
        self.assertTrue(self.storage.clear())

        mylist = list()
        mylist.append('2\t2')
        self.storage.put(mylist)

        mylist = list()
        for i in range(4,8):
            mylist.append(str(i) + '\t' + str(i))
        self.storage.put(mylist)
        self.assertEqual(self.storage.get('2'), '2')
        self.assertEqual(self.storage.get('7'), '7')

    def test_putManyEleShouldPass(self):
        self.assertTrue(self.storage.clear())

        mylist = list()
        for i in range(100, 268):
            mylist.append(str(i) + '\t' + str(i))
        self.storage.put(mylist)

        mylist = list()
        for i in range(1, 268):
            mylist.append(str(i) + '\t' + str(i))
        self.storage.put(mylist)

        mylist = list()
        for i in range(55, 300):
            mylist.append(str(i) + '\t' + str(i))
        self.storage.put(mylist)

    def test_get_ShouldPass(self):
        self.storage.get('1')

    def test_clear_ShouldPass(self):

        print mylist[0:1]
        self.assertTrue(self.storage.clear())

