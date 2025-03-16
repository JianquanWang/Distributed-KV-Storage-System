#!/usr/bin/env bash

cd raft-java-core/ && sh build.sh && cd ../distribute-java-cluster && sh build.sh && sh deploy.sh && cd env/client
