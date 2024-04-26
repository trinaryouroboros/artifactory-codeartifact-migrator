# Copyright 2022 Shawn Qureshi and individual contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import boto3
from botocore.config import Config
import dask
import os
import sys
import shutil
import re
import time
from . import codeartifact
from . import artifactory
from . import caching
from . import monitor
from . import boto_setup

logger = monitor.getLogger()

supported_packages = [
  'maven',
  'pypi',
  'npm'
]

# The replication path all replication processing will be saved to temporarily during replication
replication_path = '.replication'

# Codeartifact token refresh
token_refresh = 5 # hours

db_file = ""

def get_packagename(package):
  """
  get_packagename strips namespace.

  :param package: package variable to strip
  :return: name of the package
  """
  return package.split('/')[-1]

def get_package_type(repository, artifactory_repos):
  """
  get_package_type fetches the repository package manager type

  :param repository: repository name to inspect
  :param artifactory_repos: dictionary of Artifactory /api/storageinfo repositoriesSummaryList
  """
  success = False
  for repo in artifactory_repos:
    if repo['repoKey'] == repository:
      success = True
      repo_to_check = repo
      break
  if not success:
    logger.critical(f"Repository {repository} not found in Artifactory list retrieved")
    sys.exit(1)  
  if repo_to_check.get('packageType'):
    if repo_to_check['repoType'] == "LOCAL":
      return repo_to_check['packageType'].lower()
  else:
    logger.critical(f"Repo missing packageType key:\n{str(repo_to_check)}")
    sys.exit(1)

def check_artifactory_repos(repos, artifactory_repos):
  """
  check_artifactory_repos verifies a repository is in the api listing from Artifactory

  :param repos: space seperated list of repositories to check
  :param artifactory_repos: dictionary of Artifactory /api/storageinfo repositoriesSummaryList
  """
  success_all = []
  for repo in repos.split(" "):
    success = {
      'repository': repos, 
      'success': False
    }
    for repository in artifactory_repos:
      if repository['repoKey'] == repo:
        success['success'] = True
    success_all.append(success)
  for result in success_all:
    if result['success'] == True:
      logger.info(f"Repository check for {repo} in Artifactory listing succeeded")
    else:
      logger.critical(f"Repository {repo} not found in Artifactory listing")
      sys.exit(1)

def append_package_specific_keys(args, package_dict):
  """
  append_package_specific_keys adds any special keys based on package manager type.

  :param args: arguments passed to cli command
  :param package_dict: standard package dictionary to inspect
  :return: package dictionary plus any special keys
  """
  if package_dict['type'] == 'npm':
    package_dict['metadata'] = artifactory.artifactory_npm_metadata_fetch(args, package_dict)

  if package_dict['type'] == 'maven':
    package_name_split = package_dict['package'].split('/')
    package_name_split.remove(package_dict['package'].split('/')[-1])
    package_dict['namespace'] = '/'.join(package_name_split)

  return package_dict

def get_artifactory_package_versions(binaries, package_dict):
  """
  get_artifactory_package_versions gets all versions of a given package from
  Artifactory. Each package manager type is inspected and versions are returned
  based on the style of the package manager.

  :param binaries: list of binary uris from Artifactory /api/search/artifact
  :param package_dict: standard package dictionary to inspect
  :return: list of uris of the specific version
  """
  uris = []
  for uri in binaries:
    if package_dict['type'] == 'npm':
      version = uri.split('/' + package_dict.get('package') + '/')[-1].split(package_dict.get('package') + '-')[1].split('.tgz')[0].split('.json')[0]
      if version not in uris:
        uris.append(version)
    if package_dict['type'] in ['pypi', 'maven']:
      if '.pom' in uri or \
                '.jar' in uri or \
                '.tar.gz' in uri or \
                '.whl' in uri or \
                '.egg' in uri:
        version = uri.split('/' + package_dict.get('package') + '/')[-1].split('/')[0]
        if version not in uris:
          uris.append(version)
  return uris

def replicate_package(args, client, token_codeartifact, package_dict, db_file):
  """
  replicate_package fetches uris of the binaries associated with a package. It
  first checks to see if a version was specified and passes on to the next part.
  Otherwise it will first determine all versions of a supplied package and make
  a list to process later. Then it checks to see if that package version already
  exists in codeartifact. If the check reports that the artifact version was
  pushed to codeartifact, but it's status isn't Published, it will wipe that
  artifact from codeartifact and continue with replication. If it is fully
  published already in codeartifact it will skip replication. Finally, it will
  fetch the binaries associated with the package version, and upload them to
  codeartifact.

  :param args: command line arguments
  :param client: api client to use with codeartifact
  :param token_codeartifact: token generated from codeartifact
  :param package_dict: standard package dictionary to inspect
  :return success boolean:
  """
  success = True
  all_packages_published = False
  packages_to_replicate_temp = []
  if args.cache:
    # Insert package root if it's not in cache
    if not caching.check_package(args, package_dict['package'], package_dict['repository'], db_file):
      caching.insert_package(args, package_dict['package'], package_dict['repository'], db_file)
  if not package_dict.get('version'):
    all_packages_published = True
    # Get all versions of the package and append to list to replicate
    skip = False    
    if args.cache:
      if caching.check_all_versions_published(args, package_dict['package'], package_dict['repository'], db_file):
        logger.info(f"Cache: All versions of package {package_dict['package']} in repository {package_dict['repository']} already published, skipping'")
        return {'package': package_dict['package'], 'version': package_dict['version'], 'published': True}
      if caching.check_all_versions_fetched(args, package_dict['package'], package_dict['repository'], db_file):
        # Check if all versions were fetched, append if so and skip binary search
        logger.info(f"Cache: All versions of package {package_dict['package']} in repository {package_dict['repository']} already fetched, using cached list.")
        for version in caching.fetch_all_versions(args, package_dict['package'], package_dict['repository'], db_file):
          package_full = package_dict
          package_full['version'] = version
          if package_full not in packages_to_replicate_temp:
            packages_to_replicate_temp.append(package_full)
        skip = True
    if skip == False:
      # Parse each uri and generate packages to check and replicate
      for uri in artifactory.artifactory_package_binary_search(args, package_dict):
        if skip == False:
          version = ''
          if package_dict.get('type') == 'npm':
            ## ToDo: It might be better to search .npm metadata to fetch versions
            if re.search('.tgz$', uri):
              version = uri.split('/')[-1].split(package_dict.get('package'))[-1].replace('.tgz', '')
          elif package_dict.get('type') in ['pypi', 'maven']:
            version = uri.split('/' + package_dict.get('package') + '/')[1].split('/')[0]
          else:
            print('WARNING: Package type ' + package_dict.get('type') + ' not supported yet.')
          if version == '':
            print('WARNING: Unable to replicate ' + package_dict.get('package') + ': No versions found in binary search')
          else:
            package_full = package_dict
            package_full['version'] = version
            if package_full not in packages_to_replicate_temp:
              packages_to_replicate_temp.append(package_full)              
              if args.cache:
                # Insert package version if not in cache yet
                if not caching.check_package_version(args, package_dict['package'], package_dict['repository'], package_dict['version'], db_file):
                  caching.insert_package_version(args, package_dict['package'], package_dict['repository'], package_dict['version'], db_file)
    if args.cache:
      caching.set_all_versions_fetched(args, package_dict['package'], package_dict['repository'], db_file)
  else:
    if args.cache:
      # Insert package version if not in cache yet
      if not caching.check_package_version(args, package_dict['package'], package_dict['repository'], package_dict['version'], db_file):
        caching.insert_package_version(args, package_dict['package'], package_dict['repository'], package_dict['version'], db_file)
    packages_to_replicate_temp.append(package_dict)

  """
  Here we check to see if each package version already exists in codeartifact.
  If a package version exists but it's not in Published status, we delete it and
  add to the replication. If a package version does exist and it's Published, we
  skip that package version.
  """
  packages_to_replicate = []
  for temp_dict in packages_to_replicate_temp:
    skip = False
    if args.cache:      
      if caching.check_version_published(args, temp_dict['package'], temp_dict['repository'], temp_dict['version'], db_file):
        logger.info(f"Cache: Package {temp_dict['package']} version {temp_dict['version']} in repository {temp_dict['repository']} already published, skipping.")
        skip = True      

    if skip == False:
      check_result = codeartifact.codeartifact_check_package_version(args, client, temp_dict)
      if check_result == 2:
        # This means the package was not fully published and will be wiped and published
        codeartifact.codeartifact_wipe_package_version(args, client, temp_dict)
      if check_result in [1, 2]:
        packages_to_replicate.append(temp_dict)
      else:
        logger.info(f"Package version found in codeartifact with status Published, skipping: {temp_dict['repository']} {temp_dict['package']} {temp_dict['version']}")
        if args.cache:
          if not caching.check_version_published(args, temp_dict['package'], temp_dict['repository'], temp_dict['version'], db_file):
            logger.debug(f"Cache: Codeartifact already shows artifact published, setting cache to published for {temp_dict['repository']} {temp_dict['package']} {temp_dict['version']}")
            caching.set_package_version_to_published(args, temp_dict['package'], temp_dict['repository'], temp_dict['version'], db_file)

  logger.debug(f"Packages to replicate: {packages_to_replicate}")
  regex = re.compile("[$&+,:;=?#|'<>^*()%!\"\s\[\]]")
  for package in packages_to_replicate:    
    publish_error = ""
    publish_fail = False
    if re.search(regex, package['package']) or re.search(regex, package['version']):
      logger.warning(f"Bad characters found in package name or version, skipping: {package['repository']} {package['package']} {package['version']}")
      success = False
    else:
      logger.info(f"Replicating {package['repository']} {package['package']} {package['version']}")
      uris = artifactory.artifactory_package_binary_search(args, package)

      if package['type'] in supported_packages:
        foldername = package['package'].split('/')[-1] + f"-{package['version']}"
      else:
        logger.critical(f"Package type {package['type']} not supported: {package}")
        sys.exit(1)
      uri_formatted = ""
      tree = './' + replication_path + '/' + foldername
      """ToDo: We are encountering a problem where maven snapshot version binaries
      are creating their own version in codeartifact and making a mess in the UI.
      The final snapshot subversion does get set to published at the end though.
      - Pending AWS support
      """
      for uri in uris:
        uri_formatted = uri.replace('api/storage/', '')
        if args.dryrun:
          logger.info(f"Dryrun: Would download binary from Artifactory: {uri_formatted}")
          logger.info(f"Dryrun: Would upload binary to codeartifact: https://{package['endpoint']}/{package['package']}/{package['version']}")
        else:
          artifactory.artifactory_binary_fetch(args, uri_formatted, replication_path, foldername)
          response = codeartifact.codeartifact_upload_binary(args, client, token_codeartifact, package, tree + '/' + uri.split('/')[-1])
          logger.debug(f"Response: {response}")
          if not response.ok:
            publish_fail = True
            if publish_error != "":
              publish_error = publish_error + " -- "
            publish_error = publish_error + f"{response.status_code}, {response.reason}, {response.text}"
      # Required after pushing maven jar/pom's
      if package['type'] == 'maven' and publish_fail == False:        
        # This sets maven versions to published status
        uri = uri_formatted.removesuffix(uri_formatted.split('/')[-1]) + "maven-metadata.xml"
        if args.dryrun:
          logger.info(f"Dryrun: Would download binary from Artifactory: {uri}")
          logger.info(f"Dryrun: Would upload binary to codeartifact: https://{package['endpoint']}/{package['package']}/{package['version']}")
        else:
          artifactory.artifactory_binary_fetch(args, uri, replication_path, foldername)
          response = codeartifact.codeartifact_upload_binary(args, client, token_codeartifact, package, tree + '/maven-metadata.xml')
          # This is a failsafe, sometimes maven-metadata.xml doesn't force status to Published
          codeartifact.codeartifact_update_package_status(args, client, package)
      if publish_fail == True:
        logger.warning(f"Publish for {package['repository']} package {package['package']} version {package['version']} failed, response: {publish_error}")
      if args.dryrun:
        logger.info(f"Dryrun: Would clean up replication folder {tree}")
      else:
        logger.debug(f"Cleaning up {tree} locally on disk")
        try:
          shutil.rmtree(tree, ignore_errors=True)
        except Exception as exc:
          logger.warning(f"Exception on deleting {tree}: {exc}")
    # Validation here to confirm package is there in codeartifact after upload
    missing_error = f"Package {package['package']} {package['version']} was not found in codeartifact after upload with status Published. This could mean that your package version did not match semver according to AWS documentation."
    if args.dryrun:
      logger.info(f"Dryrun: Would validate package {package['package']} {package['version']} exists in codeartifact.")
    else:
      if codeartifact.codeartifact_check_package_version(args, client, package) != 0:
        logger.warning(missing_error)
        all_packages_published = False
        publish_fail = True
        if publish_error != "":
          publish_error = publish_error + " -- "
        publish_error = (publish_error + missing_error).replace("'", '"')
      success = False
      if args.cache:        
        if publish_fail == True:          
          caching.set_publish_fail(args, package['package'], package['repository'], package['version'], db_file)
          all_packages_published == False
        if publish_error != "":          
          caching.set_publish_error(args, package['package'], package['repository'], package['version'], publish_error, db_file)          
        if success == True:
          logger.debug(f"Cache: Setting package {package['repository']} {package['package']} {package['version']} to codeartifact published")
          caching.set_package_version_to_published(args, package['package'], package['repository'], package['version'], db_file)
  if args.cache:
    if all_packages_published == True:
      caching.set_all_versions_published(args, package['package'], package['repository'], db_file)
  return {'package': package_dict['package'], 'version': package_dict['version'], 'published': success}

def replicate_specific_packages(args, client, artifactory_repos, codeartifact_repos, db_file):
  """
  replicate_specific_packages replicates user specified packages. This will
  detect if user specified versions or not. If no versions specified it will
  search for all versions of an artifact name and replicate all versions.

  :param args: command line arguments
  :param client: api client to use with codeartifact
  :artifactory_repos: list of artifactory repo dicts
  :codeartifact_repos: list of current codeartifact repos
  :param db_file: database filename
  """
  package_type = get_package_type(args.repositories, artifactory_repos)
  if package_type not in supported_packages:
    logger.critical(f"Repository {args.repositories} package type {package_type} not supported.")
    sys.exit(1)
  codeartifact.codeartifact_check_create_repo(args, client, args.repositories, codeartifact_repos)
  # For specified package mode, we are only making a token once instead of refreshing it periodically
  token_codeartifact = client.get_authorization_token(
    domain = args.codeartifactdomain
  )['authorizationToken']
  if args.dryrun:
    endpoint = f"codeartifact-test-endpoint-dryrun.com/{args.repositories}"
  else:
    endpoint = codeartifact.codeartifact_get_repository_endpoint(args, client, args.repositories, package_type)
  for package in args.packages.split(" "):
    package_split = package.split(':')
    package_name = package_split[0]

    ## See if package is already in cache
    package_check = False
    if args.cache:
      if caching.check_package(args, package_name, args.repositories, db_file):
        logger.info(f"Cache: Package {args.repositories} {package_name} found in cache.")
        package_check = True

    # Check to see if packages exist in Artifactory first
    if package_check == False:
      if not artifactory.artifactory_package_search(args, package_name, args.repositories):
        logger.critical(f"Specified package {package_name} not found in Artifactory repository {args.repositories}")
        sys.exit(1)
      else:
        logger.info(f"Package {package_name} found in Artifactory repository {args.repositories}")
        if args.cache:
          if not caching.check_package(args, package_name, args.repositories, db_file):
            caching.insert_package(args, package_name, args.repositories, db_file)

    if len(package_split) > 1:
      # Replicate specific version of package
      if len(package_split) > 2:
        logger.critical(f"Malformed package specification, too many ':'")
        sys.exit(1)
      if package_split[1] == '':
        logger.critical(f"You specified a package version with ':' for package {package}. However, you left the version blank.")
        sys.exit(1)
      
      package_dict = {'repository': args.repositories, 'package': package_name, 'type': package_type, 'endpoint': endpoint}

      package_dict['version'] = package_split[1]

      package_dict = append_package_specific_keys(args, package_dict)
      
      if args.cache:
        if not caching.check_version_published(args, package_name, args.repositories, package_dict['version'], db_file):
          replicate_package(args, client, token_codeartifact, package_dict, db_file)
          caching.set_package_version_to_published(args, package_name, package_dict['repository'], package_dict['version'], db_file)
        else:          
          logger.info(f"Cache: Package {package_dict['repository']} {package_name} version {package_dict['version']} already published, skipping.")          
      else:
        replicate_package(args, client, token_codeartifact, package_dict, db_file)
    else:
      # Replicate all versions of package
      package_check = False
      # See if all versions of this package were fetched already
      if args.cache:
        if caching.check_all_versions_fetched(args, package, args.repositories, db_file):
          package_check = True
      if package_check == True:
        logger.info(f"Cache: All versions of package {args.repositories} {package_name} were already fetched.")
        if caching.check_all_versions_published(args, package_name, args.repositories, db_file):
          logger.info(f"Cache: All versions of package {args.repositories} {package_name} were already published, skipping")
        else:
          for version in caching.fetch_all_versions(args, package_name, args.repositories, db_file):
            if not caching.check_version_published(args, package_name, args.repositories, version, db_file):
              package_dict = {'repository': args.repositories, 'package': package_name, 'type': package_type, 'endpoint': endpoint}
              package_dict['version'] = version
              package_dict = append_package_specific_keys(args, package_dict)
              replicate_package(args, client, token_codeartifact, package_dict, db_file)
      else:
        logger.info(f"Getting all versions for {args.repositories} {package} to populate package dictionary")
        package_dict = {'repository': args.repositories, 'package': package_name, 'type': package_type, 'endpoint': endpoint}
        binaries = artifactory.artifactory_package_binary_search(args, package_dict)
        versions = get_artifactory_package_versions(binaries, package_dict)
        if versions == []:
          logger.warning(f"No versions of package {args.repositories} {package_dict['package']} were found in Artifactory")
        logger.info(f"Versions of package {args.repositories} {package_dict['package']} found in Artifactory: {versions}")
        for version in versions:
          package_dict = {'repository': args.repositories, 'package': package_name, 'type': package_type, 'endpoint': endpoint}
          package_dict['version'] = version
          package_dict = append_package_specific_keys(args, package_dict)
          replicate_package(args, client, token_codeartifact, package_dict, db_file)

def replicate_all_package_versions(args, client, token_codeartifact, packagerepo, db_file):
  """
  replicate_all_package_versions replicates all versions of a package

  :param args: command line arguments
  :param client: api client to use with codeartifact
  :param  token_codeartifact: the codeartifact authentication token to use
  :packagerepo: dictionary of package and repository
  :param db_file: database filename
  """
  package = packagerepo['package']
  repository = packagerepo['repository']
  package_type = packagerepo['package_type']
  endpoint = packagerepo['endpoint']

  skip = False
  versions = []
  if args.cache:
    if not args.refresh:
      if caching.check_all_versions_fetched(args, package, repository, db_file):
        versions = caching.fetch_all_versions(args, package, repository, db_file)
        skip = True

  if skip == False:
    logger.debug(f"Begin examining package versions: {package}")
    if not artifactory.artifactory_package_search(args, package, repository):
      logger.warning(f"Package {package} not found in Artifactory repository {repository}, skipping. This may just be an incorrect parse of the package search return.")
    else:
      logger.info(f"Package {package} found in Artifactory repository {repository}")
      package_dict = {'repository': repository, 'package': package, 'type': package_type, 'endpoint': endpoint}
      logger.info(f"Getting all versions for {package} to populate package dictionary")
      binaries = artifactory.artifactory_package_binary_search(args, package_dict)
      versions = get_artifactory_package_versions(binaries, package_dict)
      if args.cache:
        caching.set_all_versions_fetched(args, package, repository, db_file)

  versions_published = True
  if versions == []:
    logger.warning(f"No versions of package {package} were found in Artifactory, skipping.")
  else:
    logger.debug(f"Versions of package {package}: {versions}")      
    for version in versions:
      if args.cache:
        if not caching.check_package_version(args, package, repository, version, db_file):
          caching.insert_package_version(args, package, repository, version, db_file)
      package_dict = {'package': package, 'version': version, 'repository': repository, 'type': package_type, 'endpoint': endpoint}
      package_dict = append_package_specific_keys(args, package_dict)

      status = replicate_package(args, client, token_codeartifact, package_dict, db_file)
      if status['published'] == False:
        versions_published = False

  if args.cache:
    if versions_published == True:
      caching.set_all_versions_published(args, package, repository, db_file)

  return versions_published

def replicate_repository(args, client, repository, package_type, codeartifact_repos, db_file):
  """
  replicate_repository replicates an entire specified repository

  :param args: command line arguments
  :param client: api client to use with codeartifact
  :param repository: repository to replicate
  :param package_type: package manager type
  :codeartifact_repos: list of current codeartifact repos
  :param db_file: database filename
  """
  if package_type not in supported_packages:
    logger.warning(f"Repository {repository} package type {package_type} not supported.")
    return

  if args.dryrun:
    logger.info(f"Dryrun: Would check and create repository {repository} in codeartifact")
    endpoint = f"codeartifact-test-endpoint-dryrun.com/{repository}"
  else:    
    codeartifact.codeartifact_check_create_repo(args, client, repository, codeartifact_repos)    
    endpoint = codeartifact.codeartifact_get_repository_endpoint(args, client, repository, package_type)

  package_list = []

  skip = False

  if args.cache:
    if not caching.check_repository(args, repository, db_file):
      caching.insert_repository(args, repository, db_file)
    failures = caching.fetch_all_packages_with_publish_fail(args, repository, db_file)
    if failures != []:
      for package in failures:
        publish_error = caching.fetch_error_for_publish_fail(args, package[0], repository, package[1], db_file)
        logger.warning(f"Cache: Repository {repository} package {package[0]} "+\
          f"version {package[1]} encountered a publishing error previously: " +\
          f"{publish_error} -- You should fix the package so it can be " + \
          "published and try again with argument --packages specifying the " + \
          "version.")
    if caching.check_repository_all_versions_published(args, repository, db_file):
      logger.info(f"Cache: Repository {repository} already had all artifacts attempt publishing, skipping.")
      return
    if caching.check_repository_all_versions_fetched(args, repository, db_file):
      logger.info(f"Cache: Repository {repository} already had all artifacts fetched, using cache.")
      package_list = sorted(set(caching.fetch_all_packages(args, repository, db_file)))
      skip = True

  if skip == False:
    jsondata = artifactory.artifactory_http_call(args, f"/api/storage/{repository}?list&deep=1&listFolders=0")

    for file in jsondata['files']:
      if package_type == 'npm':
        # Avoid .npm metadata folders
        if not re.search('^/.npm', file['uri']):
          package_name = re.sub('^/', '', file['uri']).split('/-/')[0]
          if package_name not in package_list:
            package_list.append(package_name)
      elif package_type in ['pypi', 'maven']:
        if not re.search("maven-metadata.xml", file['uri']):
          uri_strip = re.sub('^/', '', file['uri'])
          uri_list = uri_strip.split('/')
          if len(uri_list) > 2:
            uri_list.pop(-1)
            uri_list.pop(-1)
            package_name = '/'.join(uri_list)

            if package_name not in package_list:
              package_list.append(package_name)

    package_list = sorted(set(package_list))

    if args.cache:
      for package in package_list:
        if not caching.check_package(args, package, repository, db_file):
          caching.insert_package(args, package, repository, db_file)

  logger.debug(f"Package list to replicate from Artifactory repository {repository}: {package_list}")

  token_codeartifact = client.get_authorization_token(
    domain = args.codeartifactdomain
  )['authorizationToken']
  now = int(time.time())

  proclist = []
  i = 1
  process = False
  n = 1
  versions_published = True

  for package in package_list:
    versions_published = True
    package_dict = {'package': package, 'repository': repository, 'package_type': package_type, 'endpoint': endpoint}
    # If user specified only single process, we go right to processing
    if int(args.procs) == 1:
      proclist.append(package_dict)
      process = True
    else:
      # If we reached the end of the list, finish processing
      if n == len(package_list):
        proclist.append(package_dict)
        process = True
      else:
        if i < int(args.procs):
          proclist.append(package_dict)
          i = i + 1
        else:
          proclist.append(package_dict)
          process = True

    if process == True:
      # Token refresh phase
      if int(time.time()) > now + (token_refresh * 60 * 60):
        token_codeartifact = client.get_authorization_token(
          domain = args.codeartifactdomain
        )['authorizationToken']
        now = int(time.time())

      lazy_results = []

      for item in proclist:
        # logger.info(item)        
        lazy_result = dask.delayed(replicate_all_package_versions)(args, client, token_codeartifact, item, db_file)
        lazy_results.append(lazy_result)

      for status in dask.compute(*lazy_results):
        if status == False:
          versions_published = False

      i = 1
      process = False
      proclist = []

    n = n + 1

  if args.cache:
    caching.set_repository_all_versions_fetched(args, repository, db_file)
    if versions_published == True:
      caching.set_repository_all_versions_published(args, repository, db_file)

def replicate(args):
  """
  replicate is the main function of the cli dispatch. It sets the codeartifact
  api client, and generates a list of repositories in codeartifact. Then it
  checks Artifactory access and also generates a list of repositories from it.
  We then have a few stages:
    Packages were specified in the command line:
      We run replication specifically for those packages specified.
    If only repositories were specified in command line we replicate those
      specific repositories.
    If neither repositories nor packages specified we replicate all repos.

  :param args: command line arguments
  """

  if not os.path.isdir('./' + replication_path):
    logger.debug(f"Creating directory {replication_path}")
    os.mkdir('./' + replication_path)

  db_file = f"{replication_path}/nodbfile.db"
  if args.cache:
    if args.dynamodb:
      if args.dryrun:
        db_file = f"artifactory-codeartifact-migrator-dryrun"
      else:
        db_file = f"artifactory-codeartifact-migrator-prod"
    else:
      if args.dryrun:
        db_file = f"{replication_path}/acm-dryrun.db"
      else:
        db_file = f"{replication_path}/acm-prod.db"
    caching.check_create_database(args, db_file)
    if args.clean:
      logger.info("Clean was called")
      caching.clean_cache(args, db_file)
      caching.check_create_database(args, db_file)

  aws_config = Config(
    region_name = args.codeartifactregion,
    signature_version = 'v4',
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
  )

  client = boto3.client('codeartifact', config = aws_config)

  # This checks codeartifact access and gives us a list of repos to examine
  codeartifact_repos = codeartifact.codeartifact_list_repositories(client)
  logger.debug(f"Codeartifact repo list:\n{codeartifact_repos}")

  # Then we check Artifactory access
  jsondata = artifactory.artifactory_http_call(args, '/api/storageinfo')
  artifactory_repos = jsondata['repositoriesSummaryList']
  logger.debug(f"Artifactory repo list:\n{artifactory_repos}")

  if args.packages:
    if args.repositories:
      logger.info('Specific package replication specified')
      if len(args.repositories.split(" ")) != 1:
        logger.critical('You specified packages to replicate. However you also specified multiple repositories. You can only specify one repository if specifying packages to replicate.')
        sys.exit(1)
      else:
        if args.refresh:                  
          logger.info(f"Refreshing all packages in {args.repositories}")
          caching.reset_fetched_packages(args.repositories, db_file)
        check_artifactory_repos(args.repositories, artifactory_repos)
        replicate_specific_packages(args, client, artifactory_repos, codeartifact_repos, db_file)
    else:
      logger.critical("You specified packages to replicate. However you didn't specify a repository.")
      sys.exit(1)
  elif args.repositories:
    logger.info('Specific repository replication specified')
    check_artifactory_repos(args.repositories, artifactory_repos)
    for repository in args.repositories.split(" "):
      if args.refresh:
        logger.info(f"Refreshing all packages in {repository}")
        caching.reset_fetched_packages(args, repository, db_file)
        package_type = get_package_type(repository, artifactory_repos)
      replicate_repository(args, client, repository, package_type, codeartifact_repos, db_file)
  else:
    logger.info('All repository replication specified')
    for repo in artifactory_repos:
      repository = repo['repoKey']
      if repository != "TOTAL":
        if repo['repoType'] == "LOCAL":
          if args.refresh:
            logger.info(f"Refreshing all packages in {repository}")
            caching.reset_fetched_packages(args, repository, db_file)
          package_type = get_package_type(repository, artifactory_repos)
          replicate_repository(args, client, repository, package_type, codeartifact_repos, db_file)

  if args.dryrun:
    logger.info('Dryrun operations completed')
