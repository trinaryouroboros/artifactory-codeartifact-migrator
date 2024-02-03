import argparse
import sys

def getArgs():
  """
  getArgs processes command line args and returns an argparse Namespace object
  """
  args = argparse.Namespace()

  parser = argparse.ArgumentParser()
  parser.add_argument(
    '-v',
    '--verbose',
    help = 'Increase verbosity',
    action = 'store_true'
  )
  parser.add_argument(
    '-d',
    '--debug',
    help = 'Display debug messages',
    action = 'store_true'
  )
  parser.add_argument(
    '--dryrun',
    help = 'Perform dry run operations only',
    action = 'store_true'
  )
  parser.add_argument(
    '--cache',
    help = 'Use cache mode for rerun performance',
    action = 'store_true'
  )
  parser.add_argument(
    '--dynamodb',
    help = 'Use dynamodb for cache',
    action = 'store_true'
  )
  parser.add_argument(
    '--refresh',
    help = 'Refresh all the Artifactory fetched package information in cache',
    action = 'store_true'
  )
  parser.add_argument(
    '--clean',
    help = 'Wipe the entire cache and start over',
    action = 'store_true'
  )
  parser.add_argument(
    '-p',
    '--procs',
    help = 'Number of processes to parallelize repository replication',    
    default="4"
  )
  parser.add_argument(
    '-o',
    '--output',
    help = 'Output to a file (defaults to stdout)'
  )
  parser.add_argument(
    '--artifactoryhost',
    help = 'Artifactory host name',
    required = True
  )
  parser.add_argument(
    '--artifactoryprefix',
    help = 'Artifactory host prefix if any. Example: artifactory.domain.com/myprefix/ a value "myprefix" should be set.'
  )
  parser.add_argument(
    '--artifactoryprotocol',
    help = 'Artifactory host protocol to use (http, https)',
    choices=["http", "https"],
    default="https"
  )
  parser.add_argument(
    '--artifactoryuser',
    help = 'Artifactory user name',
    required = True
  )
  parser.add_argument(
    '--artifactorypass',
    help = 'Artifactory password or api key',
    required = True
  )
  parser.add_argument(
    '--codeartifactdomain',
    help = 'Codeartifact domain',
    required = True
  )
  parser.add_argument(
    '--codeartifactaccount',
    help = 'Codeartifact account ID',
    required = True
  )
  parser.add_argument(
    '--codeartifactregion',
    help = 'Codeartifact region',
    required = True
  )
  parser.add_argument(
    '--repositories',
    help = 'Repositories to replicate from Artifactory. Example: "myrepo1 myrepo2"'
  )
  parser.add_argument(
    '--packages',
    help = 'Specify packages to replicate from Artifactory. See documentation for appropriate package values. If you specify this argument, you must supply only one repository.'
  )

  parser.parse_args(sys.argv[1:], namespace=args)

  return args
