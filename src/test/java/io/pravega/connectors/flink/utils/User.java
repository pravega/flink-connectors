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

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class User extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    private static final long serialVersionUID = 4015523217244681785L;
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"io.pravega.connectors.flink.utils\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");

    public static org.apache.avro.Schema getClassSchema() {
        return SCHEMA$;
    }

    private static SpecificData MODEL$ = new SpecificData();

    private static final BinaryMessageEncoder<User> ENCODER =
            new BinaryMessageEncoder<User>(MODEL$, SCHEMA$);

    private static final BinaryMessageDecoder<User> DECODER =
            new BinaryMessageDecoder<User>(MODEL$, SCHEMA$);

    /**
     * Return the BinaryMessageEncoder instance used by this class.
     *
     * @return the message encoder used by this class
     */
    public static BinaryMessageEncoder<User> getEncoder() {
        return ENCODER;
    }

    /**
     * Return the BinaryMessageDecoder instance used by this class.
     *
     * @return the message decoder used by this class
     */
    public static BinaryMessageDecoder<User> getDecoder() {
        return DECODER;
    }

    /**
     * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
     *
     * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
     * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
     */
    public static BinaryMessageDecoder<User> createDecoder(SchemaStore resolver) {
        return new BinaryMessageDecoder<User>(MODEL$, SCHEMA$, resolver);
    }

    /**
     * Serializes this User to a ByteBuffer.
     *
     * @return a buffer holding the serialized data for this instance
     * @throws java.io.IOException if this instance could not be serialized
     */
    public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
        return ENCODER.encode(this);
    }

    /**
     * Deserializes a User from a ByteBuffer.
     *
     * @param b a byte buffer holding serialized data for an instance of this class
     * @return a User instance decoded from the given buffer
     * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
     */
    public static User fromByteBuffer(
            java.nio.ByteBuffer b) throws java.io.IOException {
        return DECODER.decode(b);
    }

    private java.lang.CharSequence name;

    /**
     * Default constructor.  Note that this does not initialize fields
     * to their default values from the schema.  If that is desired then
     * one should use <code>newBuilder()</code>.
     */
    public User() {
    }

    /**
     * All-args constructor.
     *
     * @param name The new value for name
     */
    public User(java.lang.CharSequence name) {
        this.name = name;
    }

    public org.apache.avro.specific.SpecificData getSpecificData() {
        return MODEL$;
    }

    public org.apache.avro.Schema getSchema() {
        return SCHEMA$;
    }

    // Used by DatumWriter.  Applications should not call.
    public java.lang.Object get(int field$) {
        switch (field$) {
            case 0:
                return name;
            default:
                throw new IndexOutOfBoundsException("Invalid index: " + field$);
        }
    }

    // Used by DatumReader.  Applications should not call.
    @SuppressWarnings(value = "unchecked")
    public void put(int field$, java.lang.Object value$) {
        switch (field$) {
            case 0:
                name = (java.lang.CharSequence) value$;
                break;
            default:
                throw new IndexOutOfBoundsException("Invalid index: " + field$);
        }
    }

    /**
     * Gets the value of the 'name' field.
     *
     * @return The value of the 'name' field.
     */
    public java.lang.CharSequence getName() {
        return name;
    }


    /**
     * Sets the value of the 'name' field.
     *
     * @param value the value to set.
     */
    public void setName(java.lang.CharSequence value) {
        this.name = value;
    }

    /**
     * Creates a new User RecordBuilder.
     *
     * @return A new User RecordBuilder
     */
    public static io.pravega.connectors.flink.utils.User.Builder newBuilder() {
        return new io.pravega.connectors.flink.utils.User.Builder();
    }

    /**
     * Creates a new User RecordBuilder by copying an existing Builder.
     *
     * @param other The existing builder to copy.
     * @return A new User RecordBuilder
     */
    public static io.pravega.connectors.flink.utils.User.Builder newBuilder(io.pravega.connectors.flink.utils.User.Builder other) {
        if (other == null) {
            return new io.pravega.connectors.flink.utils.User.Builder();
        } else {
            return new io.pravega.connectors.flink.utils.User.Builder(other);
        }
    }

    /**
     * Creates a new User RecordBuilder by copying an existing User instance.
     *
     * @param other The existing instance to copy.
     * @return A new User RecordBuilder
     */
    public static io.pravega.connectors.flink.utils.User.Builder newBuilder(io.pravega.connectors.flink.utils.User other) {
        if (other == null) {
            return new io.pravega.connectors.flink.utils.User.Builder();
        } else {
            return new io.pravega.connectors.flink.utils.User.Builder(other);
        }
    }

    /**
     * RecordBuilder for User instances.
     */
    @org.apache.avro.specific.AvroGenerated
    public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<User>
            implements org.apache.avro.data.RecordBuilder<User> {

        private java.lang.CharSequence name;

        /**
         * Creates a new Builder
         */
        private Builder() {
            super(SCHEMA$);
        }

        /**
         * Creates a Builder by copying an existing Builder.
         *
         * @param other The existing Builder to copy.
         */
        private Builder(io.pravega.connectors.flink.utils.User.Builder other) {
            super(other);
            if (isValidValue(fields()[0], other.name)) {
                this.name = data().deepCopy(fields()[0].schema(), other.name);
                fieldSetFlags()[0] = other.fieldSetFlags()[0];
            }
        }

        /**
         * Creates a Builder by copying an existing User instance
         *
         * @param other The existing instance to copy.
         */
        private Builder(io.pravega.connectors.flink.utils.User other) {
            super(SCHEMA$);
            if (isValidValue(fields()[0], other.name)) {
                this.name = data().deepCopy(fields()[0].schema(), other.name);
                fieldSetFlags()[0] = true;
            }
        }

        /**
         * Gets the value of the 'name' field.
         *
         * @return The value.
         */
        public java.lang.CharSequence getName() {
            return name;
        }


        /**
         * Sets the value of the 'name' field.
         *
         * @param value The value of 'name'.
         * @return This builder.
         */
        public io.pravega.connectors.flink.utils.User.Builder setName(java.lang.CharSequence value) {
            validate(fields()[0], value);
            this.name = value;
            fieldSetFlags()[0] = true;
            return this;
        }

        /**
         * Checks whether the 'name' field has been set.
         *
         * @return True if the 'name' field has been set, false otherwise.
         */
        public boolean hasName() {
            return fieldSetFlags()[0];
        }


        /**
         * Clears the value of the 'name' field.
         *
         * @return This builder.
         */
        public io.pravega.connectors.flink.utils.User.Builder clearName() {
            name = null;
            fieldSetFlags()[0] = false;
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public User build() {
            try {
                User record = new User();
                record.name = fieldSetFlags()[0] ? this.name : (java.lang.CharSequence) defaultValue(fields()[0]);
                return record;
            } catch (org.apache.avro.AvroMissingFieldException e) {
                throw e;
            } catch (java.lang.Exception e) {
                throw new org.apache.avro.AvroRuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumWriter<User>
            WRITER$ = (org.apache.avro.io.DatumWriter<User>) MODEL$.createDatumWriter(SCHEMA$);

    @Override
    public void writeExternal(java.io.ObjectOutput out)
            throws java.io.IOException {
        WRITER$.write(this, SpecificData.getEncoder(out));
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumReader<User>
            READER$ = (org.apache.avro.io.DatumReader<User>) MODEL$.createDatumReader(SCHEMA$);

    @Override
    public void readExternal(java.io.ObjectInput in)
            throws java.io.IOException {
        READER$.read(this, SpecificData.getDecoder(in));
    }

    @Override
    protected boolean hasCustomCoders() {
        return true;
    }

    @Override
    public void customEncode(org.apache.avro.io.Encoder out)
            throws java.io.IOException {
        out.writeString(this.name);

    }

    @Override
    public void customDecode(org.apache.avro.io.ResolvingDecoder in)
            throws java.io.IOException {
        org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
        if (fieldOrder == null) {
            this.name = in.readString(this.name instanceof Utf8 ? (Utf8) this.name : null);

        } else {
            for (int i = 0; i < 1; i++) {
                switch (fieldOrder[i].pos()) {
                    case 0:
                        this.name = in.readString(this.name instanceof Utf8 ? (Utf8) this.name : null);
                        break;

                    default:
                        throw new java.io.IOException("Corrupt ResolvingDecoder.");
                }
            }
        }
    }
}










