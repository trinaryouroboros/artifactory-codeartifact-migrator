#!/bin/bash
# Refer to env.sh.template

. ./env.sh

command="pipenv run artifactory-codeartifact-migrator --artifactoryhost $ARTIFACTORY_HOST --artifactoryprefix $ARTIFACTORY_HOST_PREFIX --artifactoryuser $ARTIFACTORY_USERNAME --artifactorypass $ARTIFACTORY_PASSWORD --codeartifactdomain $CODEARTIFACT_DOMAIN --codeartifactaccount $CODEARTIFACT_ACCOUNT --codeartifactregion $CODEARTIFACT_REGION"
if [ -z $ARTIFACTORY_REPOSITORIES ]; then
  echo "Repositories not defined, will replicate all repositories."
else
  echo "Repositores defined, will replicate from specified repositories."
  command=$command" --repositories $ARTIFACTORY_REPOSITORIES"
fi

if [ -z $ARTIFACTORY_PACKAGES ]; then
  echo "Packages not defined, will replicate all packages."
else
  echo "Packages specified, will only replicate specified packages."
  command=$command" --packages $ARTIFACTORY_PACKAGES"
fi

if [ -z $ACM_DRYRUN ]; then
  echo "Production run, will perform full real replication"
else
  echo "Dry run enabled"
  command=$command" --dryrun"
fi

if [ -z $ACM_VERBOSE ]; then
  echo "Verbose not enabled"
else
  echo "Verbose enabled"
  command=$command" -v"
fi

if [ -z $ACM_DEBUG ]; then
  echo "Debug not enabled"
else
  echo "Debug enabled"
  command=$command" --debug"
fi

if [ -z $ACM_CACHE ]; then
  echo "Cache not defined, run will be fresh start."
else
  echo "Cache defined, will init/use cached database."
  command=$command" --cache"
fi

if [ -z $ACM_CLEAN ]; then
  echo "Cache clean not defined, will use cache if configured."
else
  echo "Cache clean defined, will wipe database."
  command=$command" --clean"
fi

if [ -z $ACM_REFRESH ]; then
  echo "Cache refresh not defined, will use cache if configured."
else
  echo "Cache refresh defined, will refresh packages."
  command=$command" --refresh"
fi

if [ -z $ACM_REFRESH ]; then
  echo "Refresh not defined, will use existing cache fetch information."
else
  echo "Refresh defined, will freshly fetch all information from Artifactory."
  command=$command" --refresh"
fi

if [ -z $ACM_OUTPUT ]; then
  echo "Log output not defined, logs will be sent to stdout."
else
  echo "Log output defined, logs will be sent to $ACM_OUTPUT."
  command=$command" --output $ACM_OUTPUT"
fi

if [ -z $ACM_DYNAMODB ]; then
  echo "DynamoDB not defined, will use local cache if specified."
else
  echo "DynamoDB specified, will use DynamoDB."
  command=$command" --dynamodb"
fi

if [ -z $ACM_PROCS ]; then
  echo "Procs not defined, will use default parallel procs."
else
  echo "Procs defined, will use this value for parallel procs."
  command=$command" --procs $ACM_PROCS"
fi

$command
