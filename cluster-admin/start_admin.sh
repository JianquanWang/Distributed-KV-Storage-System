#!/usr/bin/env bash

java -cp dependency/*:cluster-admin-1.0.0.jar com.raftimpl.raft.admin.AdminMain "127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" conf get

java -cp dependency/*:cluster-admin-1.0.0.jar com.raftimpl.raft.admin.AdminMain "127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" conf add "127.0.0.1:8054:4,127.0.0.1:8055:5"

java -cp dependency/*:cluster-admin-1.0.0.jar com.raftimpl.raft.admin.AdminMain "127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" conf del "127.0.0.1:8054:4,127.0.0.1:8055:5"
