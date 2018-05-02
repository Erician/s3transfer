#encoding=utf-8
import logging

log = logging.getLogger('root')

def clean_with_prefix(client, bucketName, prefix):
    marker = ''
    while True:
        returnVal = client.list_objects_without_delimiter( bucketName,prefix,marker)
        if not returnVal:
            return False
        [objects,isTruncated,marker] = returnVal
        if not client.delete_objectlist(bucketName, objects):
            return False
        if not isTruncated:
            break
    return True


                
                
                