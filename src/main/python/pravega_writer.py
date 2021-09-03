"""
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0
"""

from enum import Enum
from datetime import timedelta
from typing import Union

from py4j.java_gateway import JavaObject
from pyflink.common.serialization import SerializationSchema
from pyflink.datastream.functions import SinkFunction
from pyflink.java_gateway import get_gateway

from pravega_config import PravegaConfig, Stream


class PravegaWriterMode(Enum):
    """
    The supported modes of operation for flink's pravega writer.

    :data:`BEST_EFFORT`:
    Any write failures will be ignored hence there could be data loss.

    :data:`ATLEAST_ONCE`:
    The writer will guarantee that all events are persisted in pravega.
    There could be duplicate events written though.

    :data:`EXACTLY_ONCE`:
    The writer will guarantee that all events are persisted in pravega exactly once.

    """
    BEST_EFFORT = 0
    ATLEAST_ONCE = 1
    EXACTLY_ONCE = 2

    def _to_j_pravega_writer_mode(self) -> JavaObject:
        j_pravega_writer_mode = get_gateway().jvm \
            .io.pravega.connectors.flink.PravegaWriterMode
        return getattr(j_pravega_writer_mode, self.name)


class FlinkPravegaWriter(SinkFunction):
    """Flink sink implementation for writing into pravega storage."""

    def __init__(
        self,
        stream: Union[str, Stream],
        pravega_config: PravegaConfig,
        serialization_schema: SerializationSchema,
        enable_metrics: bool = True,
        writer_mode: PravegaWriterMode = PravegaWriterMode.ATLEAST_ONCE,
        enable_watermark: bool = False,
        txn_leader_renewal_period: timedelta = timedelta(seconds=30)
    ) -> None:
        """Build the `FlinkPravegaWriter` with options.

        NOTE: `withEventRouter` is not supported yet.

        Args:
            stream (Union[str, Stream]):
                Add a stream to be written to by the writer.

            pravega_config (PravegaConfig):
                Set the Pravega client configuration, which includes
                connection info, security info, and a default scope.

            serialization_schema (SerializationSchema):
                Sets the serialization schema.

            enable_metrics (bool, optional):
                Pravega reader metrics. Defaults to True.

            writer_mode (PravegaWriterMode, optional):
                Sets the writer mode to provide at-least-once or exactly-once
                guarantees. Defaults to PravegaWriterMode.ATLEAST_ONCE.

            enable_watermark (bool, optional):
                Enable watermark. Defaults to False.

            txn_leader_renewal_period (timedelta, optional):
                Sets the transaction lease renewal period.
                Defaults to 30 seconds on java side.

                When the writer mode is set to EXACTLY_ONCE, transactions are
                used to persist events to the Pravega stream. The transaction
                interval corresponds to the Flink checkpoint interval.
                Throughout that interval, the transaction is kept alive with a
                lease that is periodically renewed. This configuration setting
                sets the lease renewal period.
        """
        j_builder: JavaObject = get_gateway().jvm \
            .io.pravega.connectors.flink.FlinkPravegaWriter.builder()

        # AbstractWriterBuilder
        j_builder.forStream(
            stream if type(stream) == str else stream._j_stream)
        j_builder.withPravegaConfig(pravega_config._j_pravega_config)
        j_builder.enableMetrics(enable_metrics)

        # AbstractStreamingWriterBuilder
        j_builder.withWriterMode(writer_mode._to_j_pravega_writer_mode())
        j_builder.enableWatermark(enable_watermark)
        JFlinkTimeCls = get_gateway().jvm.org.apache.flink.api.common.time.Time
        j_builder.withTxnLeaseRenewalPeriod(
            JFlinkTimeCls.seconds(txn_leader_renewal_period.seconds))

        # FlinkPravegaWriter.Builder
        j_builder.withSerializationSchema(
            serialization_schema._j_serialization_schema)

        j_flink_pravega_writer: JavaObject = j_builder.build()

        super(FlinkPravegaWriter,
              self).__init__(sink_func=j_flink_pravega_writer)
