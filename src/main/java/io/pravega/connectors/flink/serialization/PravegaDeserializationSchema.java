/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.serialization;

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.serialization.DeserializationSchema;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A deserialization schema adapter for a Pravega serializer.
 * 
 * <p>This adapter exposes the Pravega serializer as a Flink Deserialization schema and
 * exposes the produced type (TypeInformation) to allow Flink to configure its internal
 * serialization and persistence stack.
 *
 * <p>An additional method {@link #extractEvent(EventRead)} is provided for
 * applying the metadata in the deserialization. This method can be overriden in the extended class. </p>
 */
public class PravegaDeserializationSchema<T> 
        implements DeserializationSchema<T>, WrappingSerializer<T> {

    // The TypeInformation of the produced type
    private final TypeInformation<T> typeInfo;

    // The Pravega serializer that backs this Flink DeserializationSchema
    private final Serializer<T> serializer;

    /**
     * Creates a new PravegaDeserializationSchema using the given Pravega serializer, and the
     * type described by the type class.
     *
     * <p>Use this constructor if the produced type is not generic and can be fully described by
     * a class. If the type is generic, use the {@link #PravegaDeserializationSchema(TypeHint, Serializer)}
     * constructor instead.
     * 
     * @param typeClass  The class describing the deserialized type.
     * @param serializer The serializer to deserialize the byte messages.
     */
    public PravegaDeserializationSchema(Class<T> typeClass, Serializer<T> serializer) {
        checkNotNull(typeClass);
        checkSerializer(serializer);

        this.serializer = serializer;

        try {
            this.typeInfo = TypeInformation.of(typeClass);
        } catch (InvalidTypesException e) {
            throw new IllegalArgumentException(
                    "Due to Java's type erasure, the generic type information cannot be properly inferred. " + 
                    "Please pass a 'TypeHint' instead of a class to describe the type. " +
                    "For example, to describe 'Tuple2<String, String>' as a generic type, use " +
                    "'new PravegaDeserializationSchema<>(new TypeHint<Tuple2<String, String>>(){}, serializer);'"
            );
        }
    }

    /**
     * Creates a new PravegaDeserializationSchema using the given Pravega serializer, and the
     * type described by the type hint.
     * 
     * <p>Use this constructor if the produced type is generic and cannot be fully described by
     * a class alone. The type hint instantiation captures generic type information to make it
     * available at runtime.
     * 
     * <pre>{@code
     * DeserializationSchema<Tuple2<String, String>> schema = 
     *     new PravegaDeserializationSchema<>(new TypeHint<Tuple2<String, String>>(){}, serializer);
     * }</pre>
     * 
     * @param typeHint The Type Hint describing the deserialized type.
     * @param serializer The serializer to deserialize the byte messages.
     */
    public PravegaDeserializationSchema(TypeHint<T> typeHint, Serializer<T> serializer) {
        this(TypeInformation.of(typeHint), serializer);
    }

    /**
     * Creates a new PravegaDeserializationSchema using the given Pravega serializer, and the
     * given TypeInformation.
     * 
     * @param typeInfo The TypeInformation describing the deserialized type.
     * @param serializer The serializer to deserialize the byte messages.
     */
    public PravegaDeserializationSchema(TypeInformation<T> typeInfo, Serializer<T> serializer) {
        checkNotNull(typeInfo, "typeInfo");
        checkSerializer(serializer);

        this.typeInfo = typeInfo;
        this.serializer = serializer;
    }

    // ------------------------------------------------------------------------

    @Override
    public T deserialize(byte[] message) throws IOException {
        ByteBuffer msg = ByteBuffer.wrap(message);
        return serializer.deserialize(msg);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return typeInfo;
    }

    @Override
    public Serializer<T> getWrappedSerializer() {
        return serializer;
    }

    /**
     * An method for applying the metadata in deserialization.
     * Override it in the custom extended {@link PravegaDeserializationSchema} if the Pravega metadata is needed.
     *
     * @param eventRead The EventRead structure the client returns which contains metadata
     *
     * @return the deserialized event with metadata
     */
    public T extractEvent(EventRead<T> eventRead) {
        return eventRead.getEvent();
    }

    // ------------------------------------------------------------------------

    private static void checkSerializer(Serializer<?> serializer) {
        checkNotNull(serializer, "serializer");
        checkArgument(serializer instanceof Serializable,
                "The serializer class must be serializable (java.io.Serializable).");
    }
}
