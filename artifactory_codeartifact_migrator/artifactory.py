#!/usr/bin/env python3
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

import json
import requests
import os
import sys
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from . import monitor
from . import boto_setup

http_timeout = 120 # seconds

logger = monitor.getLogger()

class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = http_timeout
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)

retry_strategy = Retry(
    total=10,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"],
    backoff_factor=1
)

# Make an API Call to Artifactory and return json
def artifactory_http_call(args, api_path):
  """
  artifactory_http_call makes an API Call to Artifactory.

  :param args: arguments passed to cli command
  :param api_path: api path to add to url call
  :return: json data of the http response text
  """
  artifactory_auth = (args.artifactoryuser, args.artifactorypass)
  session = requests.session()
  session.auth = (
    artifactory_auth
  )
  session.mount("http://", TimeoutHTTPAdapter(max_retries=retry_strategy))
  session.mount("https://", TimeoutHTTPAdapter(max_retries=retry_strategy))
  if args.artifactoryprefix:
    prefix = f"/{args.artifactoryprefix}"
  else:
    prefix = ""
  uri = f"{args.artifactoryprotocol}://{args.artifactoryhost}{prefix}{api_path}"

  response = session.get(
      uri
  )

  if response.status_code == 200:
    return json.loads(response.text)
  else:
    logger.critical(f"Failure connecting to {uri} : {str(response)}")
    sys.exit(1)

def artifactory_package_search(args, package, repository):
  """
  artifactory_package_search searches Artifactory to verify a package exists.

  :param args: arguments passed to cli command
  :param package: api path to add to url call
  :param repository: repository to scans
  :return: boolean of success
  """
  
  package_search = artifactory_http_call(args, '/api/storage/' + repository + '/' + package)
  success = False
  if repository + '/' + package in package_search.get('uri'):
    success = True
  return success

def artifactory_package_binary_search(args, package_dict):
  """
  artifactory_package_binary_search fetches all binaries associated with a
  package in Artifactory.

  :param args: arguments passed to cli command
  :param package_dict: standard package dictionary to inspect
  :return: list of binary uri's
  """  
  
  binaries = []  
  if package_dict['type'] == 'pypi':
    if args.artifactoryprefix:
      prefix = f"/{args.artifactoryprefix}"
    else:
      prefix = ""
    uri = f"{args.artifactoryprotocol}://{args.artifactoryhost}{prefix}/{package_dict['repository']}/{package_dict['package'].split('/')[-1]}"
    binary_search = artifactory_http_call(args, f"/api/storage/{package_dict['repository']}/{package_dict['package'].split('/')[-1]}?list&deep=1")    
    if binary_search.get('files'):
      for i in binary_search.get('files'):
        if package_dict.get('version'):         
          if '/' + package_dict.get('version') + '/' in i['uri']:
            # Avoid files that aren't standard binaries
            ## ToDo: This should just be a regex search with $ end
            if '.tar.gz' in i['uri'] or \
              '.whl' in i['uri'] or \
              '.egg' in i['uri']:
              binaries.append(f"{uri}{i['uri']}")
        else:
          binaries.append(f"{uri}{i['uri']}")
    else:
      logger.info(f"No files found in Artifactory for {package_dict['repository']} {package_dict['package']}")  
  else:
    binary_search = artifactory_http_call(args, '/api/search/artifact?name=' + package_dict['package'].split('/')[-1] + '&repos=' + package_dict['repository'])    
    for i in binary_search['results']:
      if '/' + package_dict.get('package') + '/' in i['uri']:
        if package_dict.get('version'):
          if package_dict['type'] == 'npm':
            if package_dict['package'].split('/')[-1] + '-' + package_dict.get('version') + '.tgz' in i['uri']:
              binaries.append(i['uri'])
          elif package_dict['type'] in ['pypi', 'maven']:
            if '/' + package_dict.get('version') + '/' in i['uri']:
              if not 'maven-metadata.xml' in i['uri']:
                # Avoid files that aren't standard binaries
                ## ToDo: This should just be a regex search with $ end
                if '.pom' in i['uri'] or \
                  '.jar' in i['uri'] or \
                  '.tar.gz' in i['uri']:
                  binaries.append(i['uri'])
          else:
            logger.critical(f"ERROR: Package type {package_dict['type']} not supported: {package_dict}")
            sys.exit(1)
        else:
          binaries.append(i['uri'])
  logger.debug(f"Binaries discovered:\n{binaries}")  
  return binaries

def artifactory_binary_fetch(args, package_path, replication_path, folder):
  """
  artifactory_binary_fetch fetches all binaries associated with a
  package in Artifactory and downloads them to a subfolder.

  :param args: arguments passed to cli command
  :param package_path: uri of binary to fetch
  :param replication_path: temporary folder root to download binary to
  :param folder: subfolder to download to in replication path
  """
  artifactory_auth = (args.artifactoryuser, args.artifactorypass)

  session = requests.session()

  session.auth = (
    artifactory_auth
  )

  uri = package_path

  response = session.get(
      uri
  )

  folder_path = './' + replication_path + '/' + folder

  if not os.path.isdir(folder_path):
    os.mkdir(folder_path)

  file_path = folder_path + '/' + package_path.split('/')[-1]

  logger.debug(f"Binary Path: {file_path}")

  fd = open(file_path, 'wb')
  for chunk in response.iter_content(chunk_size=128):
      fd.write(chunk)

  if os.path.exists(file_path):
    return True
  else:
    return False

def artifactory_npm_metadata_fetch(args, package_dict):
  """
  artifactory_npm_metadata_fetch fetches specific npm metadata for a package
  in Artifactory.

  :param args: arguments passed to cli command
  :param package_dict: standard package dictionary to inspect
  :return: json data of http response
  """
  response = artifactory_http_call(args, '/' + package_dict['repository'] + '/.npm/' + package_dict['package'] + '/package.json')
  return response
