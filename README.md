# Artifactory CodeArtifact Migrator (ACM)
## _Making it easy to move from Artifactory to AWS CodeArtifact_

Artifactory CodeArtifact Migrator (ACM) is a tool which enables you to easily move all
your artifacts from Artifactory to AWS CodeArtifact.

## Features

- Migrate packages individually either all versions or specify versions
- Migrate single or multiple repositories
- Migrate the entire Artifactory system
- Dryrun capabilities

## Installation

ACM requires Python 3.7 <= version < 3.13 to run. 

Using pipenv is the recommended way to install and run ACM.

Install the dependencies:

```sh
pipenv install
```

Install ACM:

```sh
pipenv run python setup.py install
```

Run ACM:

```sh
pipenv run artifactory-codeartifact-migrator -h
```

To simplify run operations, copy env.sh.template to env.sh, modify it per your
settings, and then execute run.sh instead.

Alternatively, you can install the required dependencies natively with pip, and
run the program using your locally installed python:

```sh
pip install -r requirements.txt
python setup.py install
artifactory-codeartifact-migrator -h
```

## Caching

ACM can cache requests and publishing status for packages and repositories with
the `--cache` option. This is handy in case there's communication issues. You
will not have to start all over again with fetching packages and publishing and
can just use the cache from the previous run.

For dryruns, `acm-dryrun.db` sqlite database will be used in the `.replication`
folder.

For production runs, `acm-prod.db` sqlite database will be used in the
`.replication` folder.

You can manage the databases manually with sqlite if you must.

If you have new packages added to Artifactory since the last run, and wish to
refresh the cached packages that were fetched, use `--refresh` option.

If you wish to start over with a clean cache use the `--clean` option.

Options `--refresh` or `--clean` will not do anything without the `--cache`
option set.

## DynamoDB

For a small amount of repositories, using the local sqlite caching is fine. 
However, if you're moving a lot of artifacts you may want to employ the power of
DynamoDB for rapid i/o and other features. We've included an option you can use:
`--dynamodb` which automatically creates DynamoDB tables on the same account 
your CodeArtifact exists. Keep in mind you should pay attention to permissions 
for the AWS account being used for the migrator.

If using `--dryrun` all DynamoDB options would happen on the following tables:

artifactory-codeartifact-migrator-dryrun-packages
artifactory-codeartifact-migrator-dryrun-repositories

Otherwise, the production DynamoDB tables will be:

artifactory-codeartifact-migrator-prod-packages
artifactory-codeartifact-migrator-prod-repositories

Keep in mind, the `--dynamodb` parameter will not work without the `--cache`
parameter specified in command line.

## Performance

ACM makes a lot of API calls to Artifactory and CodeArtifact.

We've included multi process option `--procs` to allow you to specify the
number of processes to use for all API operations. We've tested this up to 100
processes so far successfully in a Kubernetes cluster and found the migration
for millions of artifacts to go smoothly and quite fast.

There is also a known issue specifically with npm metadata. The way Artifactory
handles package queries with npm, is that it locks mysql repeatedly for every
single version of a package found. If there are a very large number of versions
of that artifact it could cause delays.

This can mean searching and executing can take some time between packages.

## Session

For small repositories or specific package replication, an AWS session is fine.
However, if you have a very large replication load you may find that the default
AWS codeartifact token refresh may be impacted by your session expiration time.
For those instances, it's recommended you use a service account with permanent
access key, or a permanent role for your instance or clusters to prevent such
token generation from failing eventually.

## Connectivity

If you have very large Artifactory repositories, and you are running this from
a local system which uses VPN and expires every day or so, you may want to
consider running this from a server unaffected by such restrictions.

## Cost Considerations

If your Artifactory system is already in AWS EC2 or Kubernetes,
it may pay for you to use a spot EC2 instance in AWS or even a Kubernetes
cronjob and run the migrator there, as you don't pay for internal AWS traffic.

## Development

Want to contribute? Great!

We recommend using the --dryrun option to validate your code executes as desired
and test on a real CodeArtifact instance for success.

Please update the version in __init__.py and tag a release when updating, based on semver.

## License

Apache License

**Free Software**
