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
import base64
import requests_toolbelt
from twine import package as package_file
from . import monitor

logger = monitor.getLogger()

def mocked_requests_get(*args, **kwargs):
  """
  mocked_requests_get returns a false on ok status for a response object
  """
  class MockResponse:
      def __init__(self):
          self.json_data = '{}'
          self.status_code = '400'
          self.reason = 'missing_from_metadata'
          self.text = 'missing_from_metadata'
          self.ok = False

  return MockResponse()

def codeartifact_list_repositories(client):
  """
  codeartifact_list_repositories fetches all repositories associated with
  codeartifact.

  :param client: api client object for aws codeartifact
  :return: response http object
  """
  success = True
  response = client.list_repositories()
  if not response.get('ResponseMetadata', {}).get('HTTPStatusCode') or response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) != 200:
    success = False
  if not success:
    logger.critical(f"Failure listing repositories:\n {response}")
    sys.exit(1)
  return response

def codeartifact_check_package_version(args, client, package_dict):
  """
  codeartifact_check_package_version inspects versions of a package in
  codeartifact and sees if they exist, or are Published status.

  :param args: arguments passed to cli command
  :param client: api client object for aws codeartifact
  :param package_dict: standard package dictionary to inspect
  :return: int response result
  """
  if package_dict['type'] == 'npm':    
    package_split = package_dict['package'].split('/')
    package = package_split[-1]
    if len(package_split) > 1:
      package_dict['namespace'] = package_split[0].replace('@', '')
    ## Note: This replacement was required at a point, need to inspect conversions
    # package = package_dict['package'].replace('_', '-').lower()
    logger.debug(f"Package stripped: {package}")
  if package_dict['type'] == 'maven':
    package_dict['namespace'] = package_dict['namespace'].replace('/', '.')
    package = package_dict['package']
  if package_dict['type'] == 'pypi':
    package = package_dict['package'].lower().replace('_', '-')
  if package_dict.get("namespace"):
    try:
      response = client.describe_package_version(
          domain=args.codeartifactdomain,
          repository=package_dict.get('repository'),
          format=package_dict.get('type'),
          namespace=package_dict.get("namespace"),
          package=package.split('/')[-1],
          packageVersion=package_dict.get('version')
      )
    except:
      logger.debug(f"Package not found in Codeartifact: {package_dict['package']} {package_dict['version']}")
      return 1
  else:
    try:
      response = client.describe_package_version(
        domain=args.codeartifactdomain,
        repository=package_dict.get('repository'),
        format=package_dict.get('type'),
        package=package,
        packageVersion=package_dict.get('version')
      )
    except:
      logger.debug(f"Package not found in Codeartifact: {package_dict['package']} {package_dict['version']}")
      return 1

  if response['packageVersion']['status'] == 'Published':
    logger.debug(f"Package exists in Codeartifact and is fully published: {package_dict['package']} {package_dict['version']}")
    return 0
  else:
    logger.debug(f"Package exists in Codeartifact and not fully published: {package_dict['package']} {package_dict['version']}")
    return 2

def codeartifact_create_repository(args, client, repository):
  """
  codeartifact_create_repository creates a codeartifact repository.

  :param args: arguments passed to cli command
  :param client: api client object for aws codeartifact
  :param repository: repository to create
  """
  if args.dryrun:
    logger.info(f"Dryrun: Would create repository {repository} here")
  else:
    response = client.create_repository(
      domain=args.codeartifactdomain,
      repository=repository
    )
    logger.debug(f"Reponse for creating {repository} on codeartifact:\n{response}")

def codeartifact_check_create_repo(args, client, repository, codeartifact_repos):
  """
  codeartifact_check_create_repo sees if a repository exists in codeartifact and
  creates it if not.

  :param args: arguments passed to cli command
  :param client: api client object for aws codeartifact
  :param repository: repository to create
  :param codeartifact_repos: dict of codeartifact repos
  """
  codeartifact_repo_exists = False
  for repo in codeartifact_repos['repositories']:
    if repo['name'] == repository:
      logger.info(f"Repository {repository} found on codeartifact")
      codeartifact_repo_exists = True
  if not codeartifact_repo_exists:
    codeartifact_create_repository(args, client, repository)

def codeartifact_get_repository_endpoint(args, client, repository, format):
  """
  codeartifact_get_repository_endpoint fetches the proper repository endpoint.

  :param args: arguments passed to cli command
  :param client: api client object for aws codeartifact
  :param repository: repository to fetch endpoint of
  :param format: package manager type
  :return: string of http endpoint
  """
  try:
    response = client.get_repository_endpoint(
        domain=args.codeartifactdomain,
        repository=repository,
        format=format
    )
    repo_dict = json.loads(str(response).replace("\'", "\""))
    return repo_dict['repositoryEndpoint']
  except:
    logger.critical(f"Unable to get Codeartifact repository endpoint for {repository}")
    sys.exit(1)

def codeartifact_wipe_package_version(args, client, package_dict):
  """
  codeartifact_wipe_package_version deletes a package version in codeartifact.

  :param args: arguments passed to cli command
  :param client: api client object for aws codeartifact
  :param package_dict: standard package dictionary to inspect
  """
  if args.dryrun:
    logger.info(f"Dryrun: Would wipe package version in codeartifact: {package_dict}")
  else:
    package = package_dict['package'].split('/')[-1]
    if package_dict.get('namespace'):
      response = client.delete_package_versions(
        domain=args.codeartifactdomain,
        repository=package_dict.get('repository'),
        format=package_dict.get('type'),
        namespace=package_dict.get('namespace'),
        package=package,
        versions=[
            package_dict.get('version'),
        ]
      )
    else:
      response = client.delete_package_versions(
        domain=args.codeartifactdomain,
        repository=package_dict.get('repository'),
        format=package_dict.get('type'),
        package=package_dict.get('package'),
        versions=[
            package_dict.get('version'),
        ]
      )
    logger.debug(f"Response from codeartifact to wipe package {package_dict['package']} version {package_dict['version']}: {response}")

def codeartifact_update_package_status(args, client, package_dict):
  """
  codeartifact_update_package_status updates package status to Published in
  codeartifact.

  :param args: arguments passed to cli command
  :param client: api client object for aws codeartifact
  :param package_dict: standard package dictionary to inspect
  """
  if package_dict.get('namespace'):
    response = client.update_package_versions_status(
      domain=args.codeartifactdomain,
      repository=package_dict.get('repository'),
      format=package_dict.get('type'),
      namespace=package_dict.get('namespace').replace('/', '.'),
      package=package_dict.get("package").split('/')[-1],
      versions=[package_dict.get('version'),],
      targetStatus='Published'
    )
  else:
    response = client.update_package_versions_status(
      domain=args.codeartifactdomain,
      repository=package_dict.get('repository'),
      format=package_dict.get('type'),
      package=package_dict.get("package"),
      versions=[package_dict.get('version'),],
      targetStatus='Published'
    )
  update_status_dict = json.loads(str(response).replace("\'", "\""))
  logger.info(f"Update status for package {package_dict.get('package')}: {update_status_dict.get('ResponseMetadata').get('HTTPStatusCode')}")

def codeartifact_upload_npm(token_codeartifact, package_dict, binary):
  """
  codeartifact_upload_npm uploads an npm type package to codeartifact.

  :param token_codeartifact: codeartifact token to use
  :param package_dict: standard package dictionary to inspect
  :param binary: local binary to upload
  :return: http response object
  """  
  data = package_dict['metadata']

  filename = binary.split('/')[-1]
  file_package = package_dict['package'] + '-' + package_dict['version'] + '.tgz'  

  with open(binary, "rb") as fp:    
    # _rev key must be removed from metadata before publishing
    data.pop('_rev', None)

    # Update tarball location for codeartifact
    # logger.debug(f"{package_dict['version']} {package_dict['endpoint']}{package_dict['package']}/{filename}")
    # logger.debug(f"Data: {data}")
    if not data['versions'].get(package_dict['version']):
      logger.warning(f"Package {package_dict['package']} version {package_dict['version']} in repo {package_dict['repository']} not found in metadata. Skipping upload.")
      return mocked_requests_get()
    data['versions'][package_dict['version']]['dist']['tarball'] = f"{package_dict['endpoint']}{package_dict['package']}/{filename}"

    # We attach our tarball with details here
    data['_attachments'] = {}
    data['_attachments'][file_package] = {}
    data['_attachments'][file_package]['content_type'] = 'application/octet-stream'
    data['_attachments'][file_package]['data'] = base64.b64encode(fp.read()).decode('ascii')
    data['_attachments'][file_package]['length'] = str(os.path.getsize(binary))

    # The metadata by default will list all versions, we only want to publish a single version metadata
    version_spec = data['versions'][package_dict['version']]
    data['versions'] = {}
    data['versions'][package_dict['version']] = version_spec    

    session = requests.session()

    session.auth = (
      ("aws", token_codeartifact)
    )

    url = package_dict['endpoint'] + package_dict['package'].replace('/', '%2f')    

    response = session.put(
        url,
        data = json.dumps(data),
        allow_redirects = False,
        headers = {"Content-Type": 'application/json'}
    )

  fp.close()  

  return response

def convert_data_to_list_of_tuples(data):
  """
  convert_data_to_list_of_tuples converts metadata_dictionary type object to
  a list of tuples.

  :param data: metadata_dictionary type object
  :return: list of tuples
  """
  data_to_send = []
  for key, value in data.items():
    if key in {"gpg_signature", "content"} or not isinstance(value, (list, tuple)):
      data_to_send.append((key, value))
    else:
      for item in value:
        data_to_send.append((key, item))
  return data_to_send

def codeartifact_upload_pypi(token_codeartifact, package_dict, binary):
  """
  codeartifact_upload_pypi uploads a pypi type package to codeartifact.

  :param token_codeartifact: codeartifact token to use
  :param package_dict: standard package dictionary to inspect
  :param binary: local binary to upload
  :return: http response object
  """
  file = package_file.PackageFile.from_filename(binary, comment=None)

  data = file.metadata_dictionary()
  data.update(
    {
      ":action": "file_upload",
      "protocol_version": "1",
    }
  )

  data_to_send = convert_data_to_list_of_tuples(data)

  with open(file.filename, "rb") as fp:
    data_to_send.append(
      ("content", (file.basefilename, fp, "application/octet-stream"))
    )
    encoder = requests_toolbelt.MultipartEncoder(data_to_send)

    session = requests.session()

    session.auth = (
      ("aws", token_codeartifact)
    )

    url = package_dict['endpoint']

    response = session.post(
        url,
        data = encoder,
        allow_redirects = False,
        headers = {"Content-Type": encoder.content_type}
    )

  fp.close()

  return response

def codeartifact_upload_maven(token_codeartifact, package_dict, binary):
  """
  codeartifact_upload_maven uploads a maven type package to codeartifact.

  :param token_codeartifact: codeartifact token to use
  :param package_dict: standard package dictionary to inspect
  :param binary: local binary to upload
  :return: http response object
  """
  binary_name = binary.split('/')[-1]
  headers = {
      'Content-Type': 'application/octet-stream'
  }
  url = package_dict['endpoint'] + \
    package_dict.get('package') + '/' + \
    package_dict.get('version') + '/' + binary_name
  response = requests.put(url, auth=('aws', token_codeartifact), data=open(binary, 'rb'), headers=headers)
  return response

def codeartifact_upload_binary(args, client, token_codeartifact, package_dict, binary):
  """
  codeartifact_upload_binary is a single purpose method to upload packages to
  codeartifact based on package type supplied.

  :param args: arguments passed to cli command
  :param client: api client object for aws codeartifact
  :param token_codeartifact: codeartifact token to use
  :param package_dict: standard package dictionary to inspect
  :param binary: local binary to upload
  :return: requests response object
  """
  logger.info(f"Publishing package {package_dict['repository']} {package_dict['package']} {package_dict['version']} - {binary}")
  if package_dict['type'] == 'npm':
    response = codeartifact_upload_npm(token_codeartifact, package_dict, binary)
  elif package_dict['type'] == 'pypi':
    response = codeartifact_upload_pypi(token_codeartifact, package_dict, binary)
  elif package_dict['type'] == 'maven':
    response = codeartifact_upload_maven(token_codeartifact, package_dict, binary)
  else:    
    logger.critical(f"Package type {package_dict['type']} not supported")
    sys.exit(1)
  return response
