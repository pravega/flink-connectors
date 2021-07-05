/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.formats.registry;

import io.pravega.schemaregistry.contract.data.SerializationFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * Table format factory for providing configured instances of Pravega-Registry to Flink RowData {@link
 * SerializationSchema} and {@link DeserializationSchema}.
 */
public class PravegaRegistryFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "pravega-registry";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        final String namespace = formatOptions.get(PravegaRegistryOptions.NAMESPACE);
        final String groupId = formatOptions.get(PravegaRegistryOptions.GROUP_ID);
        final URI schemaRegistryURI = URI.create(formatOptions.get(PravegaRegistryOptions.URI));

        final boolean failOnMissingField = formatOptions.get(PravegaRegistryOptions.FAIL_ON_MISSING_FIELD);
        final boolean ignoreParseErrors = formatOptions.get(PravegaRegistryOptions.IGNORE_PARSE_ERRORS);
        TimestampFormat timestampOption = JsonOptions.getTimestampFormat(formatOptions);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType producedDatatype) {
                final RowType rowType = (RowType) producedDatatype.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(producedDatatype);
                return new PravegaRegistryRowDataDeserializationSchema(
                        rowType,
                        rowDataTypeInfo,
                        namespace,
                        groupId,
                        schemaRegistryURI,
                        failOnMissingField,
                        ignoreParseErrors,
                        timestampOption);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        final String namespace = formatOptions.get(PravegaRegistryOptions.NAMESPACE);
        final String groupId = formatOptions.get(PravegaRegistryOptions.GROUP_ID);
        final URI schemaRegistryURI = URI.create(formatOptions.get(PravegaRegistryOptions.URI));
        final SerializationFormat serializationFormat = SerializationFormat.valueOf(
                formatOptions.get(PravegaRegistryOptions.FORMAT));

        TimestampFormat timestampOption = JsonOptions.getTimestampFormat(formatOptions);
        final JsonOptions.MapNullKeyMode mapNullKeyMode =
                JsonOptions.getMapNullKeyMode(formatOptions);
        final String mapNullKeyLiteral = formatOptions.get(PravegaRegistryOptions.MAP_NULL_KEY_LITERAL);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                return new PravegaRegistryRowDataSerializationSchema(
                        rowType,
                        namespace,
                        groupId,
                        schemaRegistryURI,
                        serializationFormat,
                        timestampOption,
                        mapNullKeyMode,
                        mapNullKeyLiteral);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PravegaRegistryOptions.URI);
        options.add(PravegaRegistryOptions.NAMESPACE);
        options.add(PravegaRegistryOptions.GROUP_ID);
        options.add(PravegaRegistryOptions.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PravegaRegistryOptions.FAIL_ON_MISSING_FIELD);
        options.add(PravegaRegistryOptions.IGNORE_PARSE_ERRORS);
        options.add(PravegaRegistryOptions.TIMESTAMP_FORMAT);
        options.add(PravegaRegistryOptions.MAP_NULL_KEY_MODE);
        options.add(PravegaRegistryOptions.MAP_NULL_KEY_LITERAL);
        return options;
    }
}
