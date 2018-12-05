<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# Metrics

Pravega metrics are collected and exposed via Flink metrics framework when using [`FlinkPravegaReader`](streaming.md#flinkpravegareader) or [`FlinkPravegaWriter`](streaming.md#flinkpravegawriter).


## Reader Metrics

The following metrics are exposed for `FlinkPravegaReader` related operations:

Name                |Description|
|-----------------|-----------------------------------------------------------------------|
|`readerGroupName`|The name of the Reader Group.|
|`scope`|The scope name of the Reader Group.|
|`streams`|The fully qualified name (i.e., `scope/stream`) of the streams that are part of the Reader Group.|
|`onlineReaders`|The readers that are currently online/available.|
|`segmentPositions`|The `StreamCut` information that indicates where the readers have read so far.|
|`unreadBytes`|The total number of bytes that have not been read yet.|

## Writer Metrics

For `FlinkPravegaWriter` related operations, only the stream name is exposed:

Name                |Description|
|-----------------|-----------------------------------------------------------------------|
|`streams`        |The fully qualified name of the stream i.e., `scope/stream`|

## Querying Metrics

The metrics can be viewed either from Flink UI or using the Flink `REST` API (like below):

```java
curl -i -s -f /jobs/<JOB-ID>/vertices/<SOURCE-TASK-ID>/metrics?get=0.Source__<SOURCE-OPERATOR-NAME>.PravegaReader.readerGroup.readerGroupName

curl -i -s -f /jobs/<JOB-ID>/vertices/<SOURCE-TASK-ID>/metrics?get=0.Source__<SOURCE-OPERATOR-NAME>.PravegaReader.readerGroup.scope

curl -i -s -f /jobs/<JOB-ID>/vertices/<SOURCE-TASK-ID>/metrics?get=0.Source__<SOURCE-OPERATOR-NAME>.PravegaReader.readerGroup.streams

curl -i -s -f /jobs/<JOB-ID>/vertices/<SOURCE-TASK-ID>/metrics?get=0.Source__<SOURCE-OPERATOR-NAME>.PravegaReader.readerGroup.onlineReaders

curl -i -s -f /jobs/<JOB-ID>/vertices/<SOURCE-TASK-ID>/metrics?get=0.Source__<SOURCE-OPERATOR-NAME>.PravegaReader.readerGroup.stream.test.segmentPositions

curl -i -s -f /jobs/<JOB-ID>/vertices/<SOURCE-TASK-ID>/metrics?get=0.Source__<SOURCE-OPERATOR-NAME>.PravegaReader.readerGroup.unreadBytes

```
