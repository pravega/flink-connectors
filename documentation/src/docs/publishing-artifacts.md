<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Publishing Artifacts

Pravega/Flink connector artifacts are published in the following repositories.

 - **Snapshot Artifacts** can be found in `jcenter -> OJO`.

 - **Release Artifacts** can be found in `Sonatype -> Maven Central`.


Pravega/Flink connector artifacts are used by projects like [pravega-samples](https://github.com/pravega/pravega-samples) which are used by external users. The `master` branch of the [pravega-samples](https://github.com/pravega/pravega-samples) repository will always point to a stable release version of Pravega/Flink connector.

 However, the development branch (`develop`) of [pravega-samples](https://github.com/pravega/pravega-samples/tree/develop) are likely to be unstable due to the Pravega/Flink connector snapshot artifact dependency. (_As the development branch of Pravega/Flink connector could possibly introduce a breaking change_).

A typical version label of a snapshot artifact will have the label `-SNAPSHOT` associated with it (for e.g., `0.1.0-SNAPSHOT`). There could be more revisions for a snapshot version and the `maven/gradle` build scripts is responsible for fetching the most recent version from the available list of revisions. The snapshot repositories are usually configured to automatically discard the old revisions based on some retention policy settings. Any downstream projects that are depending on snapshots are faced with the challenges of keeping up with the snapshot changes since they could possibly introduce any breaking changes.

To overcome this issue, the Pravega/Flink connector snapshot artifacts are published to `jcenter` repository with a **stable** version label which can be referred by any downstream projects. This will guarantee the build stability of the downstream projects that has a dependency on `flink-connectors` snapshot artifacts. However, unlike typical maven snapshots that are refreshed automatically upon any new revisions, the downstream projects are expected to synchronize with any latest snapshot revisions from `flink-connectors` by manually updating the build to make use of the new revisions.


## Publishing Snapshots

The snapshots are the artifacts that are coming from `master` branch (development branch). The snapshot artifacts are published automatically to `jcenter` through Travis build setup. Any updates to `master` branch will trigger a build that will publish the artifacts upon successful completion of the build.

We use `bintray` account to publish snapshot artifacts. Here is the [link](https://oss.jfrog.org/jfrog-dependencies/io/pravega/pravega-connectors-flink_2.11/) to the published snapshot artifacts. The gradle task that is used to publish the artifacts is provided below.

```
./gradlew clean assemble publishToRepo -PpublishUrl=jcenterSnapshot -PpublishUsername=<user> -PpublishPassword=<password>

```

The `bintray` credentials are encrypted using `travis encrypt` tool and are created for the namespace `pravega/pravega`.

```
/usr/local/bin/travis encrypt BINTRAY_USER=<BINTRAY_USER> --adapter net-http

/usr/local/bin/travis encrypt BINTRAY_KEY=<BINTRAY_KEY> --adapter net-http

```

## Publishing Release Artifacts

We use **Sonatype -> Maven Central** repository to manage the release artifacts. Please follow [How-to-release](how-to-release.md) page to understand the complete steps required to release a Pravega/Flink connector version.

Here is the gradle task that is used to publish the artifacts. The published artifacts can be found [here](https://mvnrepository.com/artifact/io.pravega) for more information.

```
./gradlew clean assemble publishToRepo -PdoSigning=true -Psigning.password=<signing-password> -PpublishUrl=mavenCentral -PpublishUsername=<sonatype-username> -PpublishPassword=<sonatype-password>
```
