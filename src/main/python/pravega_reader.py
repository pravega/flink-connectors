"""
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0
"""

from datetime import timedelta
from typing import Optional, Union

from py4j.java_gateway import JavaObject
from pyflink.common.serialization import DeserializationSchema
from pyflink.datastream.functions import SourceFunction
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_j_flink_time

from pravega_config import PravegaConfig, Stream


class StreamCut():
    """A set of segment/offset pairs for a single stream that represent
    a consistent position in the stream.

    NOTE: Only UNBOUNDED stream cut is supported, users who wish to use
    a particular stream cut need to wrap the `StreamCutImpl` manually or
    call the `from_base64` classmethod.
    """

    _j_stream_cut: JavaObject

    _unbounded: 'StreamCut'

    def __init__(self, j_stream_cut: JavaObject) -> None:
        """Add stream cut implementation to the internal java object.

        Args:
            _j_stream_cut (JavaObject): Unbounded stream cut implementation
                for now, but also could be `StreamCutImpl` if added manually.
        """
        self._j_stream_cut = j_stream_cut

    @classmethod
    def from_base64(cls, base64: str) -> 'StreamCut':
        """Obtains the a StreamCut object from its Base64 representation
        obtained via `asText()` in java.

        Args:
            base64 (str):
                Base64 representation of StreamCut obtained using `asText()`.

        Returns:
            StreamCut: The StreamCut object.
        """
        j_stream_cut = getattr(
            get_gateway().jvm.io.pravega.client.stream.StreamCut,
            'from')(base64)
        return cls(j_stream_cut)

    @classmethod
    def UNBOUNDED(cls) -> 'StreamCut':
        """This is used represents an unbounded StreamCut.
        This is used when the user wants to refer to the current HEAD of the
        stream or the current TAIL of the stream.

        Returns:
            StreamCut: The UNBOUNDED StreamCut object.
        """
        if not hasattr(cls, '_unbounded'):
            # get the java object after the connector jar is loaded
            j_stream_cut = get_gateway(
            ).jvm.io.pravega.client.stream.StreamCut.UNBOUNDED
            cls._unbounded = cls(j_stream_cut)

        return cls._unbounded


class FlinkPravegaReader(SourceFunction):
    """Flink source implementation for reading from Pravega storage."""
    def __init__(self,
                 stream: Union[str, Stream],
                 pravega_config: PravegaConfig,
                 deserialization_schema: DeserializationSchema,
                 start_stream_cut: StreamCut = StreamCut.UNBOUNDED(),
                 end_stream_cut: StreamCut = StreamCut.UNBOUNDED(),
                 enable_metrics: bool = True,
                 uid: Optional[str] = None,
                 reader_group_scope: Optional[str] = None,
                 reader_group_name: Optional[str] = None,
                 reader_group_refresh_time: timedelta = timedelta(seconds=3),
                 checkpoint_initiate_timeout: timedelta = timedelta(seconds=5),
                 event_read_timeout: timedelta = timedelta(seconds=1),
                 max_outstanding_checkpoint_request: int = 3) -> None:
        """Build the `FlinkPravegaReader` with options.

        NOTE: `withDeserializationSchemaFromRegistry` and
        `withTimestampAssigner` are not supported yet.

        Args:
            stream (Union[str, Stream]):
                Add a stream to be read by the source,
                from the earliest available position in the stream.

            pravega_config (PravegaConfig):
                Set the Pravega client configuration, which includes
                connection info, security info, and a default scope.

            deserialization_schema (DeserializationSchema):
                Sets the deserialization schema.

            start_stream_cut (StreamCut, optional):
                Read from the given start position in the stream.
                Defaults to StreamCut.UNBOUNDED.

            end_stream_cut (StreamCut, optional):
                Read from the given start position in the stream
                to the given end position. Defaults to StreamCut.UNBOUNDED.

            enable_metrics (bool, optional):
                Pravega reader metrics. Defaults to True.

            uid (Optional[str], optional):
                Configures the source uid to identify the checkpoint state
                of this source. Defaults to generated random uid on java side.

            reader_group_scope (Optional[str], optional):
                Configures the reader group scope for synchronization
                purposes. Defaults to `pravega_config.default_scope`.

            reader_group_name (Optional[str], optional):
                Configures the reader group name.
                Defaults to auto-generated name on java side.

            reader_group_refresh_time (timedelta, optional):
                Sets the group refresh time.
                Defaults to 3 seconds on java side.

            checkpoint_initiate_timeout (timedelta, optional):
                Sets the timeout for initiating a checkpoint in Pravega.
                Defaults to 5 seconds on java side.

            event_read_timeout (timedelta, optional):
                Sets the timeout for the call to read events from Pravega.
                After the timeout expires (without an event being returned),
                another call will be made. Defaults to 1 second on java side.

            max_outstanding_checkpoint_request (int, optional):
                Configures the maximum outstanding checkpoint requests to
                Pravega. Defaults to 3 on java side.

                Upon requesting more checkpoints than the specified maximum,
                (say a checkpoint request times out on the
                ReaderCheckpointHook but Pravega is still working on it),
                this configurations allows Pravega to limit any further
                checkpoint request being made to the ReaderGroup.

                This configuration is particularly relevant when multiple
                checkpoint requests need to be honored (e.g., frequent
                savepoint requests being triggered concurrently).
        """
        j_builder: JavaObject = get_gateway().jvm \
            .io.pravega.connectors.flink.FlinkPravegaReader.builder()

        # AbstractReaderBuilder
        j_builder.forStream(
            stream if type(stream) == str else stream._j_stream,
            start_stream_cut._j_stream_cut, end_stream_cut._j_stream_cut)
        j_builder.withPravegaConfig(pravega_config._j_pravega_config)
        j_builder.enableMetrics(enable_metrics)

        # FlinkPravegaReader.Builder
        j_builder.withDeserializationSchema(
            deserialization_schema._j_deserialization_schema)

        # AbstractStreamingReaderBuilder
        if uid: j_builder.uid(uid)
        if reader_group_scope:
            j_builder.withReaderGroupScope(reader_group_scope)
        if reader_group_name:
            j_builder.withReaderGroupName(reader_group_name)
        j_builder.withReaderGroupRefreshTime(
            to_j_flink_time(reader_group_refresh_time))
        j_builder.withCheckpointInitiateTimeout(
            to_j_flink_time(checkpoint_initiate_timeout))
        j_builder.withEventReadTimeout(to_j_flink_time(event_read_timeout))
        j_builder.withMaxOutstandingCheckpointRequest(
            max_outstanding_checkpoint_request)

        j_flink_pravega_reader: JavaObject = j_builder.build()

        super(FlinkPravegaReader,
              self).__init__(source_func=j_flink_pravega_reader)
