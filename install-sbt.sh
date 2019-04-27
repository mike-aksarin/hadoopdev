#!/usr/bin/env bash

# docker cp bintray--sbt-rpm.repo cdh:etc/yum.repos.d

yum install java-1.8.0-openjdk
yum -y install ca-certificates openssl nss
yum install sbt
