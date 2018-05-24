#encode=utf-8
from collections import deque
import logging
log = logging.getLogger('root')

'''
binary search tree like this:
            root
    left            right
left    right   left      right

the format of each node, and the elements of node sorted with increment:
start\tend
key\tmd5
'''

CacheCapicity = 10
NodeSize = 1000*10

class Storage:
    ROOT = 'root'
    LEFT = 'left'
    RIGHT = 'right'

    def __init__(self, client, bucketName, prefix, cacheCapicity = CacheCapicity, nodeSize = NodeSize):
        self.__client = client
        self.__bucketName = bucketName
        self.__prefix = prefix
        self.__cache = Cache(cacheCapicity)
        self.__nodeSize = nodeSize

    '''
    :param: elements is a list sorted with increment
    note:the value with the same key will be overwrite
    '''
    def put(self, elements = list()):
        if elements and len(elements) != 0:
            elements.sort(self.__cmp)
            return self.__putElements(elements, Storage.ROOT)

    '''
        find the cache first, if not, download from oss and cache it
        if the key does not exist, return ""
    '''
    def get(self, key):
        value = self.__cache.get(key)
        if value != "":
            log.info('get md5 from cache, ' + key + value)
            return value
        else:
            log.info('get md5 from storage, ' + key + value)
            node = self.__find(key, Storage.ROOT)
            if node is not None:
                start, end, nodeFile = node
                cacheEntry = CacheEntry(start, end, nodeFile)
                self.__cache.put(cacheEntry)
                return self.__cache.get(key)
            else:
                return ""

    def clear(self):
        self.__cache.clear()
        return self.__clearBinarySearchTreeInOss(Storage.ROOT)

    def __cmp(self, x, y):
        key_x = x.split('\t')[0]
        key_y = y.split('\t')[0]
        if key_x > key_y:
            return 1
        elif key_x < key_y:
            return -1
        else:
            return 0

    def __clearBinarySearchTreeInOss(self, curNodeName):
        try:
            if self.__client.does_object_exists(self.__bucketName, self.__prefix + curNodeName) is True:
                self.__client.delete_one_object(self.__bucketName, self.__prefix + curNodeName)
                self.__clearBinarySearchTreeInOss(curNodeName + '/' + Storage.LEFT)
                self.__clearBinarySearchTreeInOss(curNodeName + '/' + Storage.RIGHT)
            return True
        except Exception, e:
            log.info(e)
            return False

    def __putElements(self, elements, curNodeName):
        try:
            if self.__client.does_object_exists(self.__bucketName, self.__prefix + curNodeName) is False:
                if len(elements) > self.__nodeSize:
                    thisNodeContent = elements[len(elements) - self.__nodeSize:]
                    elements = elements[0:len(elements) - self.__nodeSize]
                else:
                    thisNodeContent = elements
                    elements = None
                start = thisNodeContent[0].split('\t')[0]
                end = thisNodeContent[len(thisNodeContent)-1].split('\t')[0]
                thisNodeContent.insert(0, start + '\t' + end)
                self.__client.put_object(self.__bucketName, self.__prefix + curNodeName, '\n'.join(thisNodeContent))
                if elements and len(elements):
                    return self.__putElements(elements, curNodeName + '/' + Storage.LEFT)
                else:
                    return True
            else:
                nodeFile = self.__client.get_object_with_stream(self.__bucketName, self.__prefix + curNodeName)
                start, end = nodeFile.readline().strip('\n').strip().split('\t')
                curNodeElements = list()
                for line in nodeFile:
                    curNodeElements.append(line.strip('\n').strip())
                mergedElements = self.__mergeList(elements, curNodeElements)
                startPos = self.__binarySearch(mergedElements, start)
                endPos = self.__binarySearch(mergedElements, end)
                if startPos != 0 and startPos != -1:
                    self.__putElements(mergedElements[0:startPos], curNodeName + '/' + Storage.LEFT)
                if endPos != len(mergedElements)-1 and endPos != -1:
                    self.__putElements(mergedElements[endPos+1:], curNodeName + '/' + Storage.RIGHT)
                if len(mergedElements[startPos:endPos+1]) > self.__nodeSize:
                    thisNodeContent = mergedElements[endPos-self.__nodeSize+1:endPos+1]
                    elements = mergedElements[startPos: endPos-self.__nodeSize+1]
                else:
                    thisNodeContent = mergedElements[startPos:endPos+1]
                    elements = None
                start = thisNodeContent[0].split('\t')[0]
                end = thisNodeContent[len(thisNodeContent) - 1].split('\t')[0]
                thisNodeContent.insert(0, start + '\t' + end)
                self.__client.put_object(self.__bucketName, self.__prefix + curNodeName, '\n'.join(thisNodeContent))
                if elements and len(elements):
                    return self.__putElements(elements, curNodeName + '/' + Storage.LEFT)
                else:
                    return True

        except Exception, e:
            log.error(e)
            return False

    def __binarySearch(self, L, key):
        i = 0
        j = len(L) - 1
        mid = (i + j) / 2
        while i <= j:
            if key == L[mid].split('\t')[0]:
                return mid
            elif key < L[mid]:
                j = mid - 1
            else:
                i = mid + 1
            mid = (i + j) / 2
        return -1

    # same element will be removed
    def __mergeList(self, LA, LB):
        i = 0
        j = 0
        mergedList = list()
        while i < len(LA) and j < len(LB):
            while i < len(LA) and j < len(LB) and LA[i] < LB[j]:
                mergedList.append(LA[i])
                i += 1

            while j < len(LB) and i < len(LA) and LB[j] < LA[i]:
                mergedList.append(LB[j])
                j += 1

            if i < len(LA) and j < len(LB) and LA[i] == LB[j]:
                mergedList.append(LA[i])
                i += 1
                j += 1

        if i >= len(LA):
            while j < len(LB):
                mergedList.append(LB[j])
                j += 1

        if j >= len(LB):
            while i < len(LA):
                mergedList.append(LA[i])
                i += 1

        return mergedList

    '''
    if the opetation of put is right, we can always find the node with key
    '''
    def __find(self, key, curNodeName):
        try:
            if self.__client.does_object_exists(self.__bucketName, self.__prefix + curNodeName) is False:
                return None
            nodeFile = self.__client.get_object_with_stream(self.__bucketName, self.__prefix + curNodeName)
            start, end = nodeFile.readline().strip('\n').strip().split('\t')
            if key < start:
                return self.__find(key, curNodeName + '/' + Storage.LEFT)
            elif key > end:
                return self.__find(key, curNodeName + '/' + Storage.RIGHT)
            else:
                return [start, end, nodeFile]
        except Exception, e:
            log.info(e)
            return None

'''
cache is for reading
'''
class Cache:
    def __init__(self, capicity):
        self.__capacity = capicity
        self.__queue = deque()

    #put cache entry, the replace algorithm is LRU
    def put(self, entry):
        if len(self.__queue) >= self.__capacity:
            self.__queue.pop()
        self.__queue.appendleft(entry)

    #if cache does not have the key, return ""
    def get(self, key):
        #search in cache
        for entry in self.__queue:
            if entry.doesHaveKey(key) is True:
                #according to LRU, change the position of entry to 0
                self.__queue.remove(entry)
                self.__queue.appendleft(entry)
                return entry.getValue(key)
        return ""

    def clear(self):
        self.__queue = deque()

class CacheEntry:
    def __init__(self, start, end, nodeFile):
        #parse the file
        self.__Map = dict()
        self.__start = start
        self.__end = end
        for line in nodeFile:
            key, value = line.strip('\n').strip().split('\t')
            self.__Map[key] = value

    def doesHaveKey(self, key):
        return key >= self.__start and key <= self.__end

    #if not find, return ""
    def getValue(self, key):
        if self.doesHaveKey(key):
            if self.__Map.has_key(key) is False:
                log.error('fatal error, we can not find the value with ' + key)
                return ""
            else:
                return self.__Map[key]
        else:
            return ""

