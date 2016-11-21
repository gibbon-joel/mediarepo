#!/usr/bin/python
def register():
    return [ 'image/jpeg', 'image/gif', 'image/png' ]

def scan(filename):
    print '%s scanning %s' %(__name__, filename)
