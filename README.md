# raft-java

## Acknowledgement
This Raft-Java was ported from Wenwei Hu's raft-java with some optimizing and improvement. Thanks to the Wenwei Hu for opening up such a great Java RAFT implementation.

## Quick Start
To deploy cluster with 3 instances locally，run：<br>
cd distribute-java-cluster && sh deploy.sh <br>
This script will deploy 3 instances in distribute-java-cluster/env named as example1、example2、example3；<br>
It also create a client floder for testing raft cluster's read-write ability.<br>
After successfully deployment，test writing operation，run：
cd env/client <br>
./bin/run_client.sh "list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello world <br>
Test reading operation：<br>
./bin/run_client.sh "list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello

## Manual
### Define data interface
```protobuf3
message SetRequest {
    string key = 1;
    string value = 2;
}
message SetResponse {
    bool success = 1;
}
message GetRequest {
    string key = 1;
}
message GetResponse {
    string value = 1;
}
```
```java
public interface ExampleService {
    Example.SetResponse set(Example.SetRequest request);
    Example.GetResponse get(Example.GetRequest request);
}
```