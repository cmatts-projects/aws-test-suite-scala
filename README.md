# AWS Test Suite for Scala

The AWS Test Suite is a testing repository for AWS services in Scala.
This repo contains sample implementations of AWS features and services along with a Localstack test implementation of
those services.

This suite includes sample implementations of services for the AWS Java SDK V2.

# Pre-requisites

* Docker must be installed and configured so that the current user can invoke containers. On Linux, this means adding
docker and the user to a docker group.
* Java 11 must be installed (highter versions are not supported by Scala at the time of writing).
* Maven 3.8+ must be installed.
* Scala 2.13 must be installed

# Build
To build and test:
```bash
mvn clean verify
```

# Services
## Coming soon... DynamoDB

## Coming soon... Cloudwatch

## Coming soon... Kinesis Streams

## Coming soon... S3

## Secrets Manager
The secrets manager samples demonstrate how to create, read and update secrets.

Features:
* Creation of a secret
* Updating a secret
* Reading a secret

## Parameter Store
The parameter store samples demonstrate how to create and read parameters.

Features:
* Creation of a parameter
* Reading a parameter

## Coming soon... SQS

## Coming soon... Lambda
