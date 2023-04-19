# 4. AWS SDK for pandas does not alter IAM permissions

Date: 2023-03-15

## Status

Accepted

## Context

AWS SDK for pandas requires permissions to execute AWS API calls. Permissions are granted using AWS Identity and
Access Management Policies that are attached to IAM entities - users or roles. 

## Decision

AWS SDK for pandas does not alter (create, update, delete) IAM permissions policies attached to the IAM entities.

## Consequences

It is users responsibility to ensure IAM entities they are using to execute the calls have the required permissions. 