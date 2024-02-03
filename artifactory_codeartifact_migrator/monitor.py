import sys
import logging
from . import argprocess

class PyLogger(logging.Logger):
    """Wrapper for logging.Logger to redirect its message to
    file, sys.stdout, or sys.stderr accordingly """

    def __init__(self, *args, **kwargs):
        super(PyLogger, self).__init__(self, *args)

        loglevel = logging.WARNING
        formatter = logging.Formatter(fmt='%(asctime)s:%(levelname)s:artifactory-codeartifact-migrator:%(message)s')
        stdoutput = True
        
        if kwargs.get('args'):
          sysargs = kwargs['args']

          if sysargs.get('verbose'):
            loglevel = logging.INFO

          if sysargs.get('debug'):
            loglevel = logging.DEBUG

          if sysargs.get('output'):
            fh = logging.FileHandler(sysargs['output'])
            fh.setLevel(loglevel)
            fh.setFormatter(formatter)
            self.addHandler(fh)
            stdoutput = False

        # Always use stderr
        error = logging.StreamHandler(stream=sys.stderr)
        error.setLevel(logging.CRITICAL)
        error.setFormatter(formatter)
        self.addHandler(error)

        if stdoutput == True:
          out = logging.StreamHandler(stream=sys.stdout)
          out.setFormatter(formatter)
          out.setLevel(loglevel)          
          self.addHandler(out)

def getLogger():

  args = argprocess.getArgs()
  sysargs = {}
  if args.debug:
    sysargs['debug'] = args.debug
  if args.verbose:
    sysargs['verbose'] = args.verbose
  if args.output:
    sysargs['output'] = args.output

  return PyLogger(args=sysargs)
