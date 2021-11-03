"""
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from typing import Optional

from py4j.java_gateway import JavaObject
from pyflink.java_gateway import get_gateway


class Stream():
    """A stream can be thought of as an unbounded sequence of events."""

    _j_stream: JavaObject

    def __init__(self, scope: str, stream: str) -> None:
        """Helper utility to create a Stream object.

        Args:
            scope (str): Scope of the stream.
            stream (str): Name of the stream.
        """
        self._j_stream = get_gateway().jvm.io.pravega.client.stream.Stream.of(
            scope, stream)


class DefaultCredentials():
    """Pravega authentication credential"""

    _j_default_credentials: JavaObject

    def __init__(self, username: str, password: str) -> None:
        """Creates a new object containing a token that is valid for
        Basic authentication scheme. The object encapsulates a token
        value comprising of a Base64 encoded value of "<username>:<password>".

        Args:
            username (str): The username.
            password (str): The password.
        """
        self._j_default_credentials = get_gateway(
        ).jvm.io.pravega.shared.security.auth.DefaultCredentials(
            username, password)


class PravegaConfig():
    """The Pravega client configuration."""

    _j_pravega_config: JavaObject

    def __init__(self,
                 uri: str,
                 scope: str,
                 schema_registry_uri: Optional[str] = None,
                 trust_store: Optional[str] = None,
                 default_scope: Optional[str] = None,
                 credentials: Optional[DefaultCredentials] = None,
                 validate_hostname: bool = True) -> None:
        """Create a pravega client configuration.

        Args:
            uri (str):
                The Pravega controller RPC URI.
            scope (str):
                The self-defined Pravega scope.
            schema_registry_uri (Optional[str], optional):
                The Pravega schema registry URI. Defaults to None.
            trust_store (Optional[str], optional):
                The truststore value. Defaults to None.
            default_scope (Optional[str], optional):
                The default Pravega scope, to resolve unqualified stream names
                and to support reader groups. Defaults to None.
            credentials (Optional[DefaultCredentials], optional):
                The Pravega credentials to use. Defaults to None.
            validate_hostname (bool, optional):
                TLS hostname validation. Defaults to True.
        """
        j_uri = get_gateway().jvm.java.net.URI.create(uri)
        self._j_pravega_config = get_gateway().jvm \
            .io.pravega.connectors.flink \
            .PravegaConfig.fromDefaults() \
            .withControllerURI(j_uri) \
            .withDefaultScope(scope) \
            .withHostnameValidation(validate_hostname)

        if schema_registry_uri:
            j_schema_registry_uri = get_gateway().jvm.java.net.URI.create(
                schema_registry_uri)
            self._j_pravega_config.withSchemaRegistryURI(j_schema_registry_uri)
        if trust_store: self._j_pravega_config.withTrustStore(trust_store)
        if default_scope:
            self._j_pravega_config.withDefaultScope(default_scope)
        if credentials:
            self._j_pravega_config.withCredentials(
                credentials._j_default_credentials)
