#!/usr/bin/python
# this is 99% taken from https://github.com/samwyse/sspp
# Super Simple Python Plugins is a minimal framework for implementing plugins in Python programs. There's no setup or anything, just create a directory and put the __init__.py file in it (turning the directory into a module). Any Python source files placed in that directory will be automatically loaded when the module is loaded. The names of the plugins will be put in the __all__ list variable. Beyond that, you can do anything you want with the plugins.
from glob import glob
from keyword import iskeyword
from os.path import dirname, join, split, splitext

basedir = dirname(__file__)

__all__ = []
for name in glob(join(basedir, '*.py')):
    module = splitext(split(name)[-1])[0]
    if not module.startswith('_') and not iskeyword(module):
        try:
            __import__(__name__+'.'+module)
        except:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning('Ignoring exception while loading the %r plug-in.', module)
        else:
            __all__.append(module)
__all__.sort()
