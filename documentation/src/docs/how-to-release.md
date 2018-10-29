<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# How to Release?

If you are releasing a version of Pravega/Flink Connector, then you must read and follow the below instructions. The steps mentioned here are based on the experience we are building across releases, if you have any queries, please feel free to open an issue on [Github
Issues](https://github.com/pravega/flink-connectors/issues) or contact us on [Slack](https://pravega-io.slack.com/).

**Note**: In case, if you are updating Flink version, please make sure, the new features or changes introduced in Flink are addressed in the connector.

# Preparing the Branch

Preparing the branch consists of making the necessary changes to the branch we will be working on as part of releasing. Following are the possible two situations:

 -  **Bug fix release**: This is a minor release version over an existing release branch.
 -  **Feature release or non-backward-compatible release**: This is a change to either the first or the middle digit, and it requires a new release branch.

## Bug Fix Release

For bug fix release, no new branch is required. First identify the branch we will be working on. It will be named `rX.Y`, (e.g., `r0.2`). The preparation consists of:

1. Changing `connectorVersion` in `gradle.properties` from `X.Y.Z-SNAPSHOT` to `X.Y.Z`. For example, if the current value is `connectorVersion=0.2.1-SNAPSHOT`, then change it to `connectorVersion=0.2.1`.
2. Merge this change.
3. Tag the commit with `vX.Y.Z-rc0`. (For example, `v0.2.1-rc0`).

Please note the following when performing step 3:
1. There are two ways to tag:
    -  **Via the command line**:

        ```
       > git checkout rX.Y
       > git tag vX.Y.Z-rc0
       > git push upstream vX.Y.Z-rc0
        ```
    Ensure that your `upstream` is set up correctly.

    -  **Via GitHub releases**: When creating a release candidate, Github automatically creates the tag if one doesn't exist. This is discussed in the [release candidate](#pushing-arelease-candidate) section.

2. It is possible that a release candidate is problematic and we need to do a new release candidate. In this case, we need to repeat this tagging step as many times as needed. Note that when creating a new release candidate tag, we do not need to update the Connector version.


## Major Release

In major release, either the middle or the most significant digit is changed. To perform major release, a new branch should be created. Please follow the below steps:
 >_For example, assume the new release to be `0.3.0`_

```
  > git checkout master
  > git tag branch-0.3
  > git push upstream branch-0.3
  > git checkout -b r0.3
  > git push upstream r0.3
```

After the above steps are performed, version changes needs to be updated to both `master` and `r0.3`:

*  In `master`, create an issue and corresponding pull request to change the `connectorVersion` in `gradle.properties` to `0.4.0-SNAPSHOT`. Note that we are bumping up the middle digit because, in our example, we are releasing `0.3.0`. If we were releasing say `1.0.0`, then we would change `connectorVersion` to `1.1.0-SNAPSHOT`.
* In `r0.3`, create an issue and corresponding pull request to change the `connectorVersion` in `gradle.properties` to `0.3.0`. Once that change is merged, we need to tag the commit point in the same way we described for a bug fix release. See instructions in the previous section.

# Pushing a release candidate

### Step 1: Create a release on GitHub

On the GitHub repository page, go to releases and create a new draft:

* Mark it as a "pre-release".
* Fill out the tag field and select the appropriate branch. Note that this is a release candidate, so the tag should look like `vX.Y.Z-rcA`

### Step 2: Build the distribution

Run the following commands:
> For example, assume the branch to be `r0.3`

```
   > git checkout r0.3
   > ./gradlew clean install
```

The files resulting from the build will be under `~/.m2/repository/io/pravega/pravega-connectors-flink_2.11/<RELEASE-VERSION>`. For each one of the `.jar` files in that directory, generate checksums (currently `md5`, `sha1`, and `sha256`). Use the following bash script:

```
#!/bin/bash

for file in ./*.jar ; do md5sum $file > $file.md5 ; done
for file in ./*.jar ; do shasum -a 1 $file > $file.sha1 ; done
for file in ./*.jar ; do shasum -a 256 $file > $file.sha256 ; done
```

>**Note**:In the future, we might want to automate the generation of checksums via gradle.

### Step 3: Upload the files to the pre-release draft

In the pre-release draft on GitHub, upload all the jar files (and its checksums) under `~/.m2/repository/io/pravega/pravega-connectors-flink_2.11/<RELEASE-VERSION>`. Follow the instructions on that appears on the Github release page, it is straightforward.

### Step 4: Release Notes

Create a release notes text file containing the following:
1. Related introductory text, highlighting the important changes featured in the release.
2. A complete list of commits and it can be obtained using the following command:
```
   > git log <commit-id-of-last-release>..<current-commit-id>
```

* `<commit-id-of-last-release>` depends on the kind of release we are doing. If it is a bug fix release, then we can use the tag of the last branch release. For new branches, we have been adding `branch-X.Y` tags at the branch point for convenience.
* `<current-commit-id>` is the commit point of the release candidate we are working on. If you have manually tagged the release candidate, then you can go ahead and use it in the above mentioned `git log` command.



Add the list to the release notes file and attach it the notes box in the release draft. See previous releases for an example of how to put together notes.

## Step 5: Publishing

The final step is to publish the release candidate by clicking on the button on the draft page. Once published, request the developers and community to validate the candidate.

# Releasing

Once you are happy with the release candidate, we can start the release process. There are two main parts for a Connector release:

 - Releasing on GitHub
 - Publishing on Sonatype -> Maven Central

## Releasing on GitHub

The process involved is similar to the creation of a release candidate as mentioned above. The following changes should be applied:
1. The tag should not have an `rcA` in it. If the successful rc is `v0.3.0-rc0`, then the release tag is `v0.3.0`.
2. Uncheck the pre-release box.

## Publishing on Sonatype -> Maven Central

For this step, we need a Sonatype account. See this [guide](http://central.sonatype.org/pages/ossrh-guide.html) for how to create a Sonatype account. Your account also needs to be associated to Pravega to be able to publish the artifacts.

Once you are ready, run the following steps:
* Perform the build using the following command:
```
./gradlew clean assemble publishToRepo -PdoSigning=true -Psigning.password=<signing-password> -PpublishUrl=mavenCentral -PpublishUsername=<sonatype-username> -PpublishPassword=<sonatype-password>

```
* Login to Nexus Repository Manager using Sonatype credentials with write access to `io.pravega` group.
* Under Build Promotion, choose the Staging Repositories, locate the staging repository that was created for the latest publish (format the `iopravega-XXXX`, like for example `iopravega-1004`).
* Select the repository and select the _Close button_ in the top menu bar. This will perform validations to ensure that the contents meets the maven requirements (contains signatures, javadocs, sources, etc.). This operation takes minimal time to complete. (Press the _Refresh button_ in the top menu bar occasionally until the operation completes).
* Once the operation completes, locate the URL field in the _Summary tab_ of the newly closed repository (it will be something like `https://oss.sonatype.org/content/repositories/iopravega-XXXX` where `XXXX` is the number of the staging repository). This should be tested to ensure that all artifacts are present and functions as expected.
* To test, for example, use the `pravega-samples` and checkout `develop` branch to verify that it can locate and build with the staging artifacts. Concretely:
    1. Change `pravegaVersion` and `connectorVersion` in `gradle.properties` to the staging version.
    2. Run `./gradlew clean build`
* After ensuring the correct working of the above mentioned procedures, Please click on the _Release button_ in the top menu bar.
* Please do wait, until it shows up in [Maven Central](http://central.sonatype.org/pages/ossrh-guide.html#SonatypeOSSMavenRepositoryUsageGuide-9.ActivateCentralSync).

## Change the Connector version

Once the release is done, create an issue and corresponding pull request to change the `connectorVersion` in `gradle.properties` to `X.Y.(Z+1)-SNAPSHOT` for the release branch.
