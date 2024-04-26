#!/usr/bin/env python3

import boto3
import hashlib
from . import monitor
from . import boto_setup

logger = monitor.getLogger()

dynamodb = boto3.client(
    'dynamodb', region_name = 'us-east-1')

def getSha(string):
  return hashlib.sha384(string.encode()).hexdigest()

def dynamodb_create_packages_table(db_file):
  dynamodb.create_table(
    TableName=f"{db_file}-packages",
    KeySchema=[
      {
        'AttributeName': 'packagekey',
        'KeyType': 'HASH'
      }
    ],
    AttributeDefinitions=[
      {
        'AttributeName': 'packagekey',        
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'package',        
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'repository',        
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'repositorypackage',        
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'version',        
        'AttributeType': 'S'
      }
    ],
    GlobalSecondaryIndexes=[
      {
        'IndexName': f"{db_file}-packages-repositories",
        'KeySchema': [
          {
            'AttributeName': 'repository',
            'KeyType': 'HASH'
          },
          {
            'AttributeName': 'package',
            'KeyType': 'RANGE'
          }
        ],
        'Projection': {
          'ProjectionType': 'KEYS_ONLY'
        },
        'ProvisionedThroughput': {
          'ReadCapacityUnits': 123,
          'WriteCapacityUnits': 123
        }
      },
      {
        'IndexName': f"{db_file}-packages-versions",
        'KeySchema': [
          {
            'AttributeName': 'repositorypackage',
            'KeyType': 'HASH'
          },
          {
            'AttributeName': 'version',
            'KeyType': 'RANGE'
          }
        ],
        'Projection': {
          'ProjectionType': 'KEYS_ONLY',
        },
        'ProvisionedThroughput': {
          'ReadCapacityUnits': 123,
          'WriteCapacityUnits': 123
        }
      },
    ],
    ProvisionedThroughput={
      'ReadCapacityUnits': 10,
      'WriteCapacityUnits': 10
    }
  )
  waiter = dynamodb.get_waiter('table_exists')
  waiter.wait(TableName=f"{db_file}-packages")

def dynamodb_create_repositories_table(db_file):
  dynamodb.create_table(
    TableName=f"{db_file}-repositories",
    KeySchema=[
      {
        'AttributeName': 'repositorykey',
        'KeyType': 'HASH'
      }
    ],
    AttributeDefinitions=[
      {
        'AttributeName': 'repositorykey',        
        'AttributeType': 'S'
      }
    ],
    ProvisionedThroughput={
      'ReadCapacityUnits': 10,
      'WriteCapacityUnits': 10
    }
  )
  waiter = dynamodb.get_waiter('table_exists')
  waiter.wait(TableName=f"{db_file}-repositories")

def dynamodb_check_create_tables(db_file):
  try:
    dynamodb.describe_table(
        TableName=f"{db_file}-packages"
    )
  except:
    logger.info(f"DynamoDB: Packages table not found in DynamoDB, creating {db_file}-packages...")
    dynamodb_create_packages_table(db_file)
  try:
    dynamodb.describe_table(
        TableName=f"{db_file}-repositories"
    )
  except:
    logger.info(f"DynamoDB: Repositories table not found in DynamoDB, creating {db_file}-repositories...")
    dynamodb_create_repositories_table(db_file)

def dynamodb_reset_fetched_packages(repository, db_file):
  repositorykey = getSha(f"{repository}")
  dynamodb.update_item(
    TableName=f"{db_file}-repositories",
    Key={
      'repositorykey': {
        'S': repositorykey
      }
    },
    AttributeUpdates={      
      'all_versions_fetched': {
        'Value': {
          'N': '0'
        }
      },
      'all_versions_published': {
        'Value': {
          'N': '0'
        }
      }
    }
  )

def dynamodb_wipe_tables(db_file):
  logger.info("DynamoDB: Wiping tables")
  try:
    dynamodb.delete_table(
      TableName=f"{db_file}-packages"
    )
    waiter = dynamodb.get_waiter('table_not_exists')
    waiter.wait(TableName=f"{db_file}-packages")
  except:
    logger.info(f"DynamoDB: {db_file}-packages not found during wipe operation")
  try:
    dynamodb.delete_table(
      TableName=f"{db_file}-repositories"
    )
    waiter = dynamodb.get_waiter('table_not_exists')
    waiter.wait(TableName=f"{db_file}-repositories")
  except:
    logger.info(f"DynamoDB: {db_file}-repositories not found during wipe operation")

def dynamodb_insert_package(package, repository, db_file):
  packagekey = getSha(f"{repository}/{package}")
  dynamodb.put_item(
    TableName=f"{db_file}-packages",
    Item={
      'packagekey': {
        'S': packagekey
      },
      'package': {
        'S': package
      },
      'repository': {
        'S': repository
      },
      'all_versions_fetched': {
        'N': '0'
      },
      'codeartifact_published': {
        'N': '0'
      },
      'all_versions_published': {
        'N': '0'
      }
    }
  )

def dynamodb_insert_package_version(package, repository, version, db_file):
  packagekey = getSha(f"{repository}/{package}:{version}")
  dynamodb.put_item(
    TableName=f"{db_file}-packages",
    Item={
      'packagekey': {
        'S': packagekey
      },
      'package': {
        'S': package
      },
      'repository': {
        'S': repository
      },
      'repositorypackage': {
        'S': repository + '/' + package
      },
      'version': {
        'S': version
      },
      'all_versions_fetched': {
        'N': '0'
      },
      'codeartifact_published': {
        'N': '0'
      },
      'all_versions_published': {
        'N': '0'
      }
    }
  )

def dynamodb_insert_repository(repository, db_file):
  repositorykey = getSha(repository)
  dynamodb.put_item(
    TableName=f"{db_file}-repositories",
    Item={
      'repositorykey': {
        'S': repositorykey
      },
      'all_versions_fetched': {
        'N': '0'
      },
      'all_versions_published': {
        'N': '0'
      },
      'publish_failed': {
        'N': '0'
      }
    }
  )

def dynamodb_set_all_versions_fetched(package, repository, db_file):
  packagekey = getSha(f"{repository}/{package}")
  dynamodb.update_item(
    TableName=f"{db_file}-packages",
    Key={
      'packagekey': {
        'S': packagekey
      }
    },
    AttributeUpdates={      
      'all_versions_fetched': {
        'Value': {
          'N': '1'
        }
      }
    }
  )

def dynamodb_set_all_versions_published(package, repository, db_file):
  packagekey = getSha(f"{repository}/{package}")
  dynamodb.update_item(
    TableName=f"{db_file}-packages",
    Key={
      'packagekey': {
        'S': packagekey
      }
    },
    AttributeUpdates={      
      'all_versions_published': {
        'Value': {
          'N': '1'
        }
      }
    }
  )

def dynamodb_check_package_exists(package, repository, db_file):
  packagekey = getSha(f"{repository}/{package}")
  try:
    query = dynamodb.get_item(
      TableName=f"{db_file}-packages",
      Key={
        'packagekey': {
          'S': packagekey
        }
      }
    )
    if query['Item']:
      return True
    else:
      return False
  except:
    return False

def dynamodb_check_package_version_exists(package, repository, version, db_file):
  packagekey = getSha(f"{repository}/{package}:{version}")
  try:
    query = dynamodb.get_item(
      TableName=f"{db_file}-packages",
      Key={
        'packagekey': {
          'S': packagekey
        }
      }
    )
    if query['Item']:
      return True
    else:
      return False
  except:
    return False

def dynamodb_set_package_version_to_published(package, repository, version, db_file):
  packagekey = getSha(f"{repository}/{package}:{version}")
  dynamodb.update_item(
    TableName=f"{db_file}-packages",
    Key={
      'packagekey': {
        'S': packagekey
      }
    },
    AttributeUpdates={      
      'codeartifact_published': {
        'Value': {
          'N': '1'
        }
      }
    }
  )

def dynamodb_set_publish_fail(package, repository, version, db_file):
  packagekey = getSha(f"{repository}/{package}:{version}")  
  dynamodb.update_item(
    TableName=f"{db_file}-packages",
    Key={
      'packagekey': {
        'S': packagekey
      }
    },
    AttributeUpdates={      
      'publish_failed': {
        'Value': {
          'N': '1'
        }
      }
    }
  )

def dynamodb_set_publish_error(package, repository, version, publish_error, db_file):
  packagekey = getSha(f"{repository}/{package}:{version}")
  dynamodb.update_item(
    TableName=f"{db_file}-packages",
    Key={
      'packagekey': {
        'S': packagekey
      }
    },
    AttributeUpdates={      
      'publish_error': {
        'Value': {
          'S': publish_error
        }
      }
    }
  )

def dynamodb_set_repository_all_versions_fetched(repository, db_file):
  repositorykey = getSha(repository)  
  dynamodb.update_item(
    TableName=f"{db_file}-repositories",
    Key={
      'repositorykey': {
        'S': repositorykey
      }
    },
    AttributeUpdates={      
      'all_versions_fetched': {
        'Value': {
          'N': '1'
        }
      }
    }
  )

def dynamodb_set_repository_all_versions_published(repository, db_file):
  repositorykey = getSha(repository)  
  dynamodb.update_item(
    TableName=f"{db_file}-repositories",
    Key={
      'repositorykey': {
        'S': repositorykey
      }
    },
    AttributeUpdates={      
      'all_versions_published': {
        'Value': {
          'N': '1'
        }
      }
    }
  )

def dynamodb_check_package_version_publish_failed(package, repository, version, db_file):
  packagekey = getSha(f"{repository}/{package}:{version}")
  try:
    query = dynamodb.get_item(
      TableName=f"{db_file}-packages",
      Key={
        'packagekey': {
          'S': packagekey
        }
      },
      AttributesToGet=[
        'publish_failed',
      ]
    )    
    if query['Item']:
      if query['Item']['publish_failed']:
        if query['Item']['publish_failed']['N']:
          if query['Item']['publish_failed']['N'] == '1':
            return True
          else:
            return False
        else:
          return False
      else:
        return False
    else:
      return False
  except:
    return False

def dynamodb_check_repository(repository, db_file):
  repositorykey = getSha(repository)
  try:
    query = dynamodb.get_item(
      TableName=f"{db_file}-repositories",
      Key={
        'repositorykey': {
          'S': repositorykey
        }
      }
    )
    if query['Item']:
      return True
    else:
      return False
  except:
    return False

## ToDo: Need to test this when publish_failed = 1
def dynamodb_check_repository_publish_failed(repository, db_file):
  repositorykey = getSha(repository)
  try:
    query = dynamodb.get_item(
      TableName=f"{db_file}-repositories",
      Key={
        'repositorykey': {
          'S': repositorykey
        }
      },
      AttributesToGet=[
        'publish_failed',
      ]
    )
    if query['Item']:
      if query['Item']['publish_failed']:
        if query['Item']['publish_failed']['N']:
          if query['Item']['publish_failed']['N'] == '1':
            return True
          else:
            return False
        else:
          return False
      else:
        return False
    else:
      return False
  except:
    return False

def dynamodb_check_all_versions_fetched(package, repository, db_file):
  packagekey = getSha(f"{repository}/{package}")
  try:
    query = dynamodb.get_item(
      TableName=f"{db_file}-packages",
      Key={
        'packagekey': {
          'S': packagekey
        }
      },
      AttributesToGet=[
        'all_versions_fetched',
      ]
    )
    if query['Item']:
      if query['Item']['all_versions_fetched']:
        if query['Item']['all_versions_fetched']['N']:
          if query['Item']['all_versions_fetched']['N'] == '1':
            return True
          else:
            return False
        else:
          return False
      else:
        return False
    else:
      return False
  except:
    return False

def dynamodb_check_repository_all_versions_fetched(repository, db_file):
  repositorykey = getSha(repository)
  try:
    query = dynamodb.get_item(
      TableName=f"{db_file}-repositories",
      Key={
        'repositorykey': {
          'S': repositorykey
        }
      },
      AttributesToGet=[
        'all_versions_fetched',
      ]
    )
    if query['Item']:
      if query['Item']['all_versions_fetched']:
        if query['Item']['all_versions_fetched']['N']:
          if query['Item']['all_versions_fetched']['N'] == '1':
            return True
          else:
            return False
        else:
          return False
      else:
        return False
    else:
      return False
  except:
    return False

def dynamodb_check_repository_all_versions_published(repository, db_file):  
  repositorykey = getSha(repository)
  try:
    query = dynamodb.get_item(
      TableName=f"{db_file}-repositories",
      Key={
        'repositorykey': {
          'S': repositorykey
        }
      },
      AttributesToGet=[
        'all_versions_published',
      ]
    )
    if query['Item']:
      if query['Item']['all_versions_published']:
        if query['Item']['all_versions_published']['N']:
          if query['Item']['all_versions_published']['N'] == '1':
            return True
          else:
            return False
        else:
          return False
      else:
        return False
    else:
      return False
  except:
    return False

def dynamodb_fetch_all_packages(repository, db_file):
  packages = []
  response = dynamodb.query(
    TableName = f"{db_file}-packages",
    IndexName = f"{db_file}-packages-repositories",
    Select = 'SPECIFIC_ATTRIBUTES',
    KeyConditionExpression = 'repository = :v_repository',
    ProjectionExpression='package',
    ExpressionAttributeValues = { ':v_repository': { 'S': repository } }
  )

  if response['Items']:
    for package in response['Items']:
      packages.append(package['package']['S'])
  return sorted(set(packages))

def dynamodb_fetch_all_versions(package, repository, db_file):
  versions = []
  response = dynamodb.query(
    TableName = f"{db_file}-packages",
    IndexName = f"{db_file}-packages-versions",
    Select = 'SPECIFIC_ATTRIBUTES',
    KeyConditionExpression = 'repositorypackage = :v_repositorypackage',
    ProjectionExpression='version',
    ExpressionAttributeValues = { ':v_repositorypackage': { 'S': repository + "/" + package } }
  )
  
  if response['Items']:
    for version in response['Items']:
      versions.append(version['version']['S'])
  return sorted(set(versions))

def dynamodb_fetch_all_versions_not_published(package, repository, db_file):
  not_published = []
  versions = dynamodb_fetch_all_versions(package, repository, db_file)
  for version in versions:
    packagekey = getSha(f"{repository}/{package}:{version}")
    query = dynamodb.get_item(
      TableName=f"{db_file}-packages",
      Key={
        'packagekey': {
          'S': packagekey
        }
      },
      AttributesToGet=[
        'codeartifact_published',
      ]
    )    
    if query['Item']:
      if query['Item']['codeartifact_published']:
        if query['Item']['codeartifact_published']['N']:
          if query['Item']['codeartifact_published']['N'] == '0':
            not_published.append(version)
  return not_published

def dynamodb_fetch_all_packages_with_publish_fail(repository, db_file):
  publish_fails = []
  packages = dynamodb_fetch_all_packages(repository, db_file)  
  for package in packages:
    versions = []
    versions = dynamodb_fetch_all_versions(package, repository, db_file)
    for version in versions:      
      packagekey = getSha(f"{repository}/{package}:{version}")
      query = dynamodb.get_item(
        TableName=f"{db_file}-packages",
        Key={
          'packagekey': {
            'S': packagekey
          }
        },
        AttributesToGet=[
          'publish_failed',
        ]
      )
      if query['Item']:
        if query['Item']['publish_failed']:
          if query['Item']['publish_failed']['N']:
            if query['Item']['publish_failed']['N'] == '1':
              publish_fails.append([package, version])
  return publish_fails

def dynamodb_fetch_error_for_publish_fail(package, repository, version, db_file):
  packagekey = getSha(f"{repository}/{package}:{version}")  
  query = dynamodb.get_item(
    TableName=f"{db_file}-packages",
    Key={
      'packagekey': {
        'S': packagekey
      }
    },
    AttributesToGet=[
      'publish_error',
    ]
  )  
  if query['Item']:
    if query['Item']['publish_error']:
      if query['Item']['publish_error']['S']:        
        return query['Item']['publish_error']['S']

def dynamodb_check_all_versions_published(package, repository, db_file):
  packagekey = getSha(f"{repository}/{package}")
  try:
    query = dynamodb.get_item(
      TableName=f"{db_file}-packages",
      Key={
        'packagekey': {
          'S': packagekey
        }
      },
      AttributesToGet=[
        'all_versions_published',
      ]
    )
    if query['Item']:
      if query['Item']['all_versions_published']:
        if query['Item']['all_versions_published']['N']:
          if query['Item']['all_versions_published']['N'] == '1':
            return True
          else:
            return False
        else:
          return False
      else:
        return False
    else:
      return False
  except:
    return False

def dynamodb_check_version_published(package, repository, version, db_file):
  packagekey = getSha(f"{repository}/{package}:{version}")
  try:
    query = dynamodb.get_item(
      TableName=f"{db_file}-packages",
      Key={
        'packagekey': {
          'S': packagekey
        }
      },
      AttributesToGet=[
        'codeartifact_published',
      ]
    )    
    if query['Item']:
      if query['Item']['codeartifact_published']:
        if query['Item']['codeartifact_published']['N']:
          if query['Item']['codeartifact_published']['N'] == '1':
            return True
          else:
            return False
        else:
          return False
      else:
        return False
    else:
      return False
  except:
    return False
