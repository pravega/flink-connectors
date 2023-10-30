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
package io.pravega.connectors.flink.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FileDescriptor.Syntax;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.deserialize.PbCodegenDeserializeFactory;
import org.apache.flink.formats.protobuf.deserialize.PbCodegenDeserializer;
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenUtils;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * {@link MessageToRowConverter} can convert protobuf message data to flink row
 * data by codegen
 * process.
 */
public class MessageToRowConverter {
    private static final Logger LOG = LoggerFactory.getLogger(MessageToRowConverter.class);
    private final Method decodeMethod;

    public MessageToRowConverter(RowType rowType, PbFormatConfig formatConfig)
            throws PbCodegenException {
        try {
            Descriptors.Descriptor descriptor = PbFormatUtils.getDescriptor(formatConfig.getMessageClassName());
            Class<?> messageClass = Class.forName(
                    formatConfig.getMessageClassName(),
                    true,
                    Thread.currentThread().getContextClassLoader());
            String fullMessageClassName = PbFormatUtils.getFullJavaName(descriptor);
            if (descriptor.getFile().getSyntax() == Syntax.PROTO3) {
                // pb3 always read default values
                formatConfig = new PbFormatConfig(
                        formatConfig.getMessageClassName(),
                        formatConfig.isIgnoreParseErrors(),
                        true,
                        formatConfig.getWriteNullStringLiterals());
            }
            PbCodegenAppender codegenAppender = new PbCodegenAppender();
            PbFormatContext pbFormatContext = new PbFormatContext(formatConfig);
            String uuid = UUID.randomUUID().toString().replaceAll("\\-", "");
            String generatedClassName = "GeneratedProtoToRow_" + uuid;
            String generatedPackageName = MessageToRowConverter.class.getPackage().getName();
            codegenAppender.appendLine("package " + generatedPackageName);
            codegenAppender.appendLine("import " + RowData.class.getName());
            codegenAppender.appendLine("import " + ArrayData.class.getName());
            codegenAppender.appendLine("import " + BinaryStringData.class.getName());
            codegenAppender.appendLine("import " + GenericRowData.class.getName());
            codegenAppender.appendLine("import " + GenericMapData.class.getName());
            codegenAppender.appendLine("import " + GenericArrayData.class.getName());
            codegenAppender.appendLine("import " + ArrayList.class.getName());
            codegenAppender.appendLine("import " + List.class.getName());
            codegenAppender.appendLine("import " + Map.class.getName());
            codegenAppender.appendLine("import " + HashMap.class.getName());
            codegenAppender.appendLine("import " + ByteString.class.getName());

            codegenAppender.appendSegment("public class " + generatedClassName + "{");
            codegenAppender.appendSegment(
                    "public static RowData "
                            + PbConstant.GENERATED_DECODE_METHOD
                            + "("
                            + fullMessageClassName
                            + " message){");
            codegenAppender.appendLine("RowData rowData=null");
            PbCodegenDeserializer codegenDes = PbCodegenDeserializeFactory.getPbCodegenTopRowDes(
                    descriptor, rowType, pbFormatContext);
            String genCode = codegenDes.codegen("rowData", "message", 0);
            codegenAppender.appendSegment(genCode);
            codegenAppender.appendLine("return rowData");
            codegenAppender.appendSegment("}");
            codegenAppender.appendSegment("}");

            String printCode = codegenAppender.printWithLineNumber();
            LOG.debug("Protobuf decode codegen: \n" + printCode);
            Class generatedClass = PbCodegenUtils.compileClass(
                    Thread.currentThread().getContextClassLoader(),
                    generatedPackageName + "." + generatedClassName,
                    codegenAppender.code());
            decodeMethod = generatedClass.getMethod(PbConstant.GENERATED_DECODE_METHOD, messageClass);
        } catch (Exception ex) {
            throw new PbCodegenException(ex);
        }
    }

    public RowData convertMessageToRow(Object messageObj) throws Exception {
        return (RowData) decodeMethod.invoke(null, messageObj);
    }
}
