#!/usr/bin/python
def register():
    return [ 'image/jpeg', 'image/gif', 'image/png' ]

def scan(filename):
    output = '%s scanning %s' %(__name__, filename)
    return output


def scanBulk(filenames_array):
    # either use a bulked pipe to something like exiftool, or cheat and iterate over array, calling the scan() function...
    output = {}
    for filename in filenames_array:
        output[filename] = {}
        result = scan(filename)
        output[filename]['foobar'] = result

    # expected output format: { 'filename' : {tagName, tagValue}, ... }
    return output
