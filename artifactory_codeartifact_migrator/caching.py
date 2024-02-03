# This is designed so you can specify multiple databases in the future

import sys
import sqlite3
from sqlite3 import Error
from . import monitor
from . import dynamodb

logger = monitor.getLogger()

def database_commit(execution, db_file):
  """
  database_commit executes a database command to commit

  :param execution: command to execute
  :param db_file: filename to create
  """
  conn = None
  logger.debug(f"Committing: {execution}")
  try:
    conn = sqlite3.connect(db_file, timeout=10)
    cur = conn.cursor()
    cur.execute(execution)
    conn.commit()
  except Error as e:
    logger.critical(e)
    sys.exit(1)
  finally:
    if conn:
      conn.close()

def database_query(query, db_file):
  """
  database_query runs a query against the database

  :param query: query to execute
  :param db_file: filename to create
  :return: rows of query
  """
  conn = None
  logger.debug(f"Querying: {query}")
  try:
    conn = sqlite3.connect(db_file, timeout=10)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
  except Error as e:
    logger.critical(e)
    sys.exit(1)
  finally:
    rows = cur.fetchall()
    if conn:
      conn.close()
    return rows

def check_create_database(args, db_file):
  """
  check_create_database creates a sqlite database for caching purposes if it
  doesn't exist and creates default table if it also doesn't exist

  :param db_file: filename to create
  """  
  if args.dynamodb:
    dynamodb.dynamodb_check_create_tables(db_file)
  else:
    command = '''CREATE TABLE IF NOT EXISTS packages (package text,
                repository text, version text, version_specified int,
                all_versions_fetched int, codeartifact_published int,
                all_versions_published int, publish_failed int,
                publish_error text)'''
    database_commit(command, db_file)
    command = '''CREATE TABLE IF NOT EXISTS repositories (repository text,
                all_versions_fetched int, all_versions_published int,
                publish_failed int)'''
    database_commit(command, db_file)

def reset_fetched_packages(args, repository, db_file):
  """
  reset_fetched_packages resets all packages fetch and published status for
  repositories table

  :param repository: the repository to reset
  :param db_file: database filename
  """
  if args.dynamodb:
    dynamodb.dynamodb_reset_fetched_packages(repository, db_file)
  else:
    command = f"""
        UPDATE repositories SET all_versions_fetched = 0 WHERE
        repository = '{repository}'
        """
    database_commit(command, db_file)
    command = f"""
        UPDATE repositories SET all_versions_published = 0 WHERE
        repository = '{repository}'
        """
    database_commit(command, db_file)

def clean_cache(args, db_file):
  """
  clean_cache wipes the entire cache database if it exists

  :param db_file: database filename
  """
  if args.dynamodb:
    dynamodb.dynamodb_wipe_tables(db_file)
  command = f"""
      DROP TABLE IF EXISTS packages
      """
  database_commit(command, db_file)
  command = f"""
      DROP TABLE IF EXISTS repositories
      """
  database_commit(command, db_file)

def insert_package(args, package, repository, db_file):
  """
  insert_package inserts a new package without version into the db

  :param package: package to insert
  :param repository: repository to insert to
  :param db_file: database filename
  """
  if args.dynamodb:
    dynamodb.dynamodb_insert_package(package, repository, db_file)
  else:
    command = f"""
        INSERT INTO packages (package,repository,version,version_specified,
        all_versions_fetched,codeartifact_published,all_versions_published) VALUES
        ('{package}','{repository}','',0,0,0,0)
        """
    database_commit(command, db_file)

def insert_package_version(args, package, repository, version, db_file):
  """
  insert_package_version inserts a package with version into the db

  :param package: package to insert
  :param repository: repository to insert to
  :param version: version to insert
  :param db_file: database filename
  """
  if args.dynamodb:    
    dynamodb.dynamodb_insert_package_version(package, repository, version, db_file)
  else:
    command = f"""
      INSERT INTO packages (package,repository,version,version_specified,
      all_versions_fetched,codeartifact_published,all_versions_published) VALUES
      ('{package}','{repository}','{version}',1,0,0,0)
      """
    database_commit(command, db_file)

def insert_repository(args, repository, db_file):
  """
  insert_repository inserts a new repository into the db

  :param repository: repository to insert
  :param db_file: database filename
  """
  if args.dynamodb:    
    dynamodb.dynamodb_insert_repository(repository, db_file)
  else:
    command = f"""
      INSERT INTO repositories (repository,all_versions_fetched,
      all_versions_published) VALUES
      ('{repository}',0,0)
      """
    database_commit(command, db_file)

def set_all_versions_fetched(args, package, repository, db_file):
  """
  set_all_versions_fetched sets a package indicating all versions were fetched

  :param package: package to set
  :param repository: repository to set
  :param db_file: database filename
  """
  if args.dynamodb:
    dynamodb.dynamodb_set_all_versions_fetched(package, repository, db_file)
  else:
    command = f"""
      UPDATE packages SET all_versions_fetched = 1 WHERE package = '{package}'
      AND repository = '{repository}' AND version_specified = 0
      """
    database_commit(command, db_file)

def set_all_versions_published(args, package, repository, db_file):
  """
  set_all_versions_published sets a package indicating all versions published

  :param package: package to set
  :param repository: repository to set
  :param db_file: database filename
  """
  if args.dynamodb:
    dynamodb.dynamodb_set_all_versions_published(package, repository, db_file)
  else:
    command = f"""
      UPDATE packages SET all_versions_published = 1 WHERE package = '{package}'
      AND repository = '{repository}' AND version_specified = 0
      """
    database_commit(command, db_file)

def set_package_version_to_published(args, package, repository, version, db_file):
  """
  set_package_version_to_published sets a package version to published

  :param package: package to update
  :param repository: repository to set
  :param version: version to update
  :param db_file: database filename
  """
  success = False
  if args.dynamodb:
    if dynamodb.dynamodb_check_package_version_exists(package, repository, version, db_file):
      success = True
  else:
    command = f"""
      SELECT version FROM packages WHERE package = '{package}' AND
      version_specified = 1 AND version = '{version}'
      """

    for row in database_query(command, db_file):
      if row[0] == version:
        success = True

  if success == False:
    insert_package_version(args, package, repository, version, db_file)

  if args.dynamodb:
    dynamodb.dynamodb_set_package_version_to_published(package, repository, version, db_file)
  else:
    command = f"""
      UPDATE packages SET codeartifact_published = 1 WHERE package = '{package}'
      AND version = '{version}' AND repository = '{repository}' AND
      version_specified = 1
      """
    database_commit(command, db_file)

def set_publish_fail(args, package, repository, version, db_file):
  """
  set_publish_fail sets a package version with failure publish status and marks
  associated repository as well

  :param package: package to update
  :param repository: repository to set
  :param version: version to update
  :param db_file: database filename
  """
  if args.dynamodb:
    dynamodb.dynamodb_set_publish_fail(package, repository, version, db_file)
  else:
    command = f"""
      UPDATE packages SET publish_failed = 1 WHERE package = '{package}'
      AND version = '{version}' AND repository = '{repository}' AND
      version_specified = 1
      """
    database_commit(command, db_file)
    command = f"""
      UPDATE repositories SET publish_failed = 1 WHERE repository = '{repository}'
      """
    database_commit(command, db_file)

def set_publish_error(args, package, repository, version, publish_error, db_file):
  """
  set_publish_error sets a package version with publish error

  :param package: package to update
  :param repository: repository to set
  :param version: version to update
  :param publish_error: error message to set
  :param db_file: database filename
  """
  if args.dynamodb:
    dynamodb.dynamodb_set_publish_error(package, repository, version, publish_error, db_file)
  else:
    command = f"""
      UPDATE packages SET publish_error = '{publish_error}' WHERE
      package = '{package}' AND version = '{version}' AND
      repository = '{repository}' AND version_specified = 1
      """
    database_commit(command, db_file)

def set_repository_all_versions_fetched(args, repository, db_file):
  """
  set_all_versions_fetched sets a package indicating all versions were fetched

  :param repository: repository to set
  :param db_file: database filename
  """
  if args.dynamodb:
    dynamodb.dynamodb_set_repository_all_versions_fetched(repository, db_file)
  else:
    command = f"""
      UPDATE repositories SET all_versions_fetched = 1 WHERE
      repository = '{repository}'
      """
    database_commit(command, db_file)

def set_repository_all_versions_published(args, repository, db_file):
  """
  set_all_versions_published sets a package indicating all versions published

  :param repository: repository to set
  :param db_file: database filename
  """
  if args.dynamodb:
    dynamodb.dynamodb_set_repository_all_versions_published(repository, db_file)
  else:
    command = f"""
      UPDATE repositories SET all_versions_published = 1 WHERE
      repository = '{repository}'
      """
    database_commit(command, db_file)

def check_package(args, package, repository, db_file):
  """
  check_package checks to see if a package exists already in cache

  :param package: package to search
  :param repository: repository to search
  :param db_file: database filename
  :return: success boolean
  """
  success = False
  if args.dynamodb:
    success = dynamodb.dynamodb_check_package_exists(package, repository, db_file)
  else:
    command = f"""
      SELECT package FROM packages WHERE repository = '{repository}' AND
      version_specified = 0
      """
    for row in database_query(command, db_file):
      if row[0] == package:
        success = True
  return success

def check_package_version(args, package, repository, version, db_file):
  """
  check_package checks to see if a package version exists already in cache

  :param package: package to search
  :param repository: repository to search
  :param version: version to search
  :param db_file: database filename
  :return: success boolean
  """
  success = False
  if args.dynamodb:
    success = dynamodb.dynamodb_check_package_version_exists(package, repository, version, db_file)
  else:
    command = f"""
      SELECT version FROM packages WHERE repository = '{repository}' AND
      version_specified = 1 AND version = '{version}'
      """
    for row in database_query(command, db_file):
      if row[0] == version:
        success = True
  return success

# def check_package_version_publish_failed(args, package, repository, version, db_file):
#   """
#   check_package_version_publish_failed checks if a package version encountered
#   a publishing failure

#   :param package: package to inspect
#   :param repository: repository to inspect
#   :param version: version to inspect
#   :param db_file: database filename
#   :return: success boolean
#   """
#   success = False
#   if args.dynamodb:
#     success = dynamodb.dynamodb_check_package_version_publish_failed(package, repository, version, db_file)
#   else:
#     command = f"""
#         SELECT publish_failed FROM packages WHERE package = '{package}' AND
#         repository = '{repository}' AND version = '{version}' AND
#         version_specified = 1
#         """
#     for row in database_query(command, db_file):
#       if row[0] == 1:
#           success = True
#   return success

def check_repository(args, repository, db_file):
  """
  check_repository checks to see if a repository exists already in cache

  :param repository: repository to search
  :param db_file: database filename
  :return: success boolean
  """
  success = False
  if args.dynamodb:
    success = dynamodb.dynamodb_check_repository(repository, db_file)
  else:
    command = f"""
      SELECT repository FROM repositories WHERE repository = '{repository}'
      """
    for row in database_query(command, db_file):
      if row[0] == repository:
        success = True
  return success

# def check_repository_publish_failed(args, repository, db_file):
#   """
#   check_repository_publish_failed checks to see if artifacts failed to publish
#   during a full repository replication run

#   :param repository: repository to inspect
#   :param db_file: database filename
#   :return: success boolean
#   """
#   success = False
#   if args.dynamodb:
#     success = dynamodb.dynamodb_check_repository_publish_failed(repository, db_file)
#   else:
#     command = f"""
#         SELECT publish_failed FROM repositories WHERE repository = '{repository}'
#         """
#     for row in database_query(command, db_file):
#       if row[0] == 1:
#           success = True
#   return success

def check_all_versions_fetched(args, package, repository, db_file):
  """
  check_all_versions_fetched checks if package had all versions fetched

  :param package: package to inspect
  :param repository: repository to inspect
  :param db_file: database filename
  :return: success boolean
  """
  success = False
  if args.dynamodb:
    success = dynamodb.dynamodb_check_all_versions_fetched(package, repository, db_file)
  else:
    command = f"""
        SELECT all_versions_fetched FROM packages WHERE package = '{package}' AND
        repository = '{repository}' AND version_specified = 0
        """
    for row in database_query(command, db_file):
      if row[0] == 1:
          success = True
  return success

def check_repository_all_versions_fetched(args, repository, db_file):
  """
  check_repository checks to see if all versions of artifacts were fetched

  :param repository: repository to search
  :param db_file: database filename
  :return: success boolean
  """
  success = False
  if args.dynamodb:
    success = dynamodb.dynamodb_check_repository_all_versions_fetched(repository, db_file)
  else:
    command = f"""
        SELECT all_versions_fetched FROM repositories WHERE
        repository = '{repository}'
        """
    for row in database_query(command, db_file):
      if row[0] == 1:
          success = True
  return success

def check_repository_all_versions_published(args, repository, db_file):
  """
  check_repository checks to see if all versions of artifacts were published

  :param repository: repository to search
  :param db_file: database filename
  :return: success boolean
  """
  success = False
  if args.dynamodb:
    success = dynamodb.dynamodb_check_repository_all_versions_published(repository, db_file)
  else:
    command = f"""
        SELECT all_versions_published FROM repositories WHERE
        repository = '{repository}'
        """
    for row in database_query(command, db_file):
      if row[0] == 1:
          success = True
  return success

def fetch_all_packages(args, repository, db_file):
  """
  fetch_all_packages fetches all packages of a specified repository

  :param repository: repository to fetch from
  :param db_file: database filename
  :return: list of packages
  """
  packages = []
  if args.dynamodb:
    packages = dynamodb.dynamodb_fetch_all_packages(repository, db_file)
  else:
    command = f"""
      SELECT package FROM packages WHERE repository = '{repository}' AND
      version_specified = 0
      """
    for row in database_query(command, db_file):
      packages.append(row[0])
  return packages

def fetch_all_versions(args, package, repository, db_file):
  """
  fetch_all_versions fetches all versions of a specified package

  :param package: package to fetch all versions from
  :param repository: repository to fetch from
  :param db_file: database filename
  :return: list of versions
  """
  version_list = []
  if args.dynamodb:
    version_list = dynamodb.dynamodb_fetch_all_versions(package, repository, db_file)
  else:
    command = f"""
      SELECT version FROM packages WHERE package = '{package}' AND
      repository = '{repository}' AND version_specified = 1
      """
    for row in database_query(command, db_file):
      version_list.append(row[0])
  return version_list

# def fetch_all_versions_not_published(args, package, repository, db_file):
#   """
#   fetch_all_versions_not_published fetches all versions not published

#   :param package: package to fetch all versions from
#   :param repository: repository to fetch from
#   :param db_file: database filename
#   :return: list of versions
#   """
#   version_list = []
#   if args.dynamodb:
#     version_list = dynamodb.dynamodb_fetch_all_versions_not_published(package, repository, db_file)
#   else:
#     command = f"""
#         SELECT version FROM packages WHERE package = '{package}' AND
#         version_specified = 1 AND codeartifact_published = 0 AND
#         repository = '{repository}'
#         """
#     for row in database_query(command, db_file):
#       version_list.append(row[0])
#   return version_list

def fetch_all_packages_with_publish_fail(args, repository, db_file):
  """
  fetch_all_packages_with_publish_fail fetches all versions that encountered
  a failure during publishing

  :param repository: repository to fetch from
  :param db_file: database filename
  :return: list of package versions that failed to publish
  """
  version_list = []
  if args.dynamodb:
    version_list = dynamodb.dynamodb_fetch_all_packages_with_publish_fail(repository, db_file)
  else:
    command = f"""
        SELECT package, version FROM packages WHERE version_specified = 1 AND
        codeartifact_published = 0 AND repository = '{repository}' AND
        publish_failed = 1
        """
    for row in database_query(command, db_file):
      version_list.append([row[0],row[1]])
  return version_list

def fetch_error_for_publish_fail(args, package, repository, version, db_file):
  """
  fetch_error_for_publish_fail fetches the associated text with a publish
  failure for a package version

  :param package: package to fetch from
  :param repository: repository to fetch from
  :param version: version to fetch from
  :param db_file: database filename
  :return: publish failure error message
  """
  if args.dynamodb:
    return dynamodb.dynamodb_fetch_error_for_publish_fail(package, repository, version, db_file)
  else:
    command = f"""
        SELECT publish_error FROM packages WHERE version_specified = 1 AND
        codeartifact_published = 0 AND repository = '{repository}' AND
        publish_failed = 1 AND package = '{package}' AND version = '{version}'
        """
    return database_query(command, db_file)

def check_all_versions_published(args, package, repository, db_file):
  """
  check_all_versions_published checks if all versions of a package were published

  :param package: package to inspect
  :param repository: repository to inspect
  :param db_file: database filename
  :return: success boolean
  """
  success = False
  if args.dynamodb:
    success = dynamodb.dynamodb_check_all_versions_published(package, repository, db_file)
  else:
    command = f"""
        SELECT all_versions_published FROM packages WHERE package =
        '{package}' AND repository = '{repository}' AND version_specified = 0
        """
    for row in database_query(command, db_file):
      if row[0] == 1:
        success = True
  return success

def check_version_published(args, package, repository, version, db_file):
  """
  check_version_published checks if a version of a package was published

  :param package: package to inspect
  :param repository: repository to inspect
  :param db_file: database filename
  :return: success boolean
  """
  success = False
  if args.dynamodb:
    success = dynamodb.dynamodb_check_version_published(package, repository, version, db_file)
  else:
    command = f"""
        SELECT codeartifact_published FROM packages WHERE package =
        '{package}' AND version = '{version}' AND version_specified = 1 AND
        repository = '{repository}'
        """
    for row in database_query(command, db_file):
      if row[0] == 1:
        success = True
  return success
