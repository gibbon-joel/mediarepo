#!/usr/bin/python
import exiftool

def register():
    return [ 'image/jpeg', 'image/gif', 'image/png' ]

def scan(filename):
    output = '%s scanning %s' %(__name__, filename)
    return output


def scanBulk(filenames_array):
    # either use a bulked pipe to something like exiftool, or cheat and iterate over array, calling the scan() function...
    output = {}
    with exiftool.ExifTool() as et:
        metadata = et.get_metadata_batch(filenames_array)
        for m in metadata:
            filename = '%s/%s'%(m['File:Directory'], m['File:FileName'])
            output[filename] = m

    # expected output format: { 'filename' : {tagName, tagValue}, ... }
    return output
