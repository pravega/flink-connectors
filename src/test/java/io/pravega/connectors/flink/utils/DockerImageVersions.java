/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.connectors.flink.utils;

/**
 * Utility class for defining the image names and versions of Docker containers used during the Java
 * tests. The names/versions are centralised here in order to make testing version updates easier,
 * as well as to provide a central file to use as a key when caching testing Docker files.
 */
public class DockerImageVersions {
    // The Pravega and Schema Registry docker image version is behind the snapshot version so that
    // we need to maintain the backwards compatibility of these two docker images.
    public static final String PRAVEGA = "pravega/pravega:0.11.0";
    public static final String SCHEMA_REGISTRY = "pravega/schemaregistry:0.4.0";
}
