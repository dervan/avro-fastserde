package com.rtbhouse.utils.avro;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.sun.codemodel.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;

public class FastDeserializerGenerator<T> extends FastDeserializerGeneratorBase<T> {

    private static final String DECODER = "decoder";

    private boolean useGenericTypes;
    private JMethod schemaMapMethod;
    private JFieldVar schemaMapField;
    private Map<Integer, Schema> schemaMap = new HashMap<>();
    private Map<String, JMethod> deserializeMethodMap = new HashMap<>();
    private Map<String, JMethod> skipMethodMap = new HashMap<>();
    private SchemaAnalyzer schemaAnalyzer;

    FastDeserializerGenerator(boolean useGenericTypes, Schema writer, Schema reader, File destination,
            ClassLoader classLoader,
            String compileClassPath) {
        super(writer, reader, destination, classLoader, compileClassPath);
        this.useGenericTypes = useGenericTypes;
        this.schemaAnalyzer = new SchemaAnalyzer(codeModel, useGenericTypes);
    }

    public FastDeserializer<T> generateDeserializer() {
        String className = getClassName(writer, reader, useGenericTypes ? "Generic" : "Specific");
        JPackage classPackage = codeModel._package(GENERATED_PACKAGE_NAME);

        try {
            deserializerClass = classPackage._class(className);

            JFieldVar readerSchemaField = deserializerClass.field(JMod.PRIVATE | JMod.FINAL, Schema.class,
                    "readerSchema");
            JMethod constructor = deserializerClass.constructor(JMod.PUBLIC);
            JVar constructorParam = constructor.param(Schema.class, "readerSchema");
            constructor.body().assign(JExpr.refthis(readerSchemaField.name()), constructorParam);

            Schema aliasedWriterSchema = Schema.applyAliases(writer, reader);
            Symbol generate = new ResolvingGrammarGenerator().generate(aliasedWriterSchema, reader);
            FieldAction fieldAction = FieldAction.fromValues(aliasedWriterSchema.getType(), true, generate);

            if (useGenericTypes) {
                schemaMapField = deserializerClass.field(JMod.PRIVATE,
                        codeModel.ref(Map.class).narrow(Integer.class).narrow(Schema.class), "readerSchemaMap");
                schemaMapMethod = deserializerClass.method(JMod.PRIVATE | JMod.FINAL,
                        void.class, "schemaMap");
                constructor.body().invoke(schemaMapMethod);
                schemaMapMethod.body().assign(schemaMapField,
                        JExpr._new(codeModel.ref(HashMap.class).narrow(Integer.class).narrow(Schema.class)));

                registerSchema(aliasedWriterSchema, readerSchemaField);
            }

            JClass readerSchemaClass = schemaAnalyzer.classFromSchema(reader);
            JClass outputClass = schemaAnalyzer.classFromSchema(aliasedWriterSchema);

            deserializerClass._implements(codeModel.ref(FastDeserializer.class).narrow(outputClass));
            JMethod deserializeMethod = deserializerClass.method(JMod.PUBLIC, readerSchemaClass, "deserialize");

            JVar result = declareContainerVariableForSchemaInBlock("result", aliasedWriterSchema, deserializeMethod.body());

            JTryBlock tryDeserializeBlock = deserializeMethod.body()._try();
            if (Schema.Type.RECORD.equals(aliasedWriterSchema.getType())) {
                processRecord(readerSchemaField, result, aliasedWriterSchema, reader, tryDeserializeBlock.body(), fieldAction);
            } else if (Schema.Type.ARRAY.equals(aliasedWriterSchema.getType())) {
                JVar schemaVariable = null;
                if (useGenericTypes) {
                    schemaVariable = schemaMapMethod.body().decl(codeModel.ref(Schema.class),
                            getVariableName("ElementSchema"),
                            readerSchemaField.invoke("getElementType"));

                    registerSchema(aliasedWriterSchema.getElementType(), schemaVariable);
                }
                processArray(schemaVariable, result, null, aliasedWriterSchema, reader, tryDeserializeBlock.body(), fieldAction);
            } else if (Schema.Type.MAP.equals(aliasedWriterSchema.getType())) {
                JVar schemaVariable = null;
                if (useGenericTypes) {
                    schemaVariable = schemaMapMethod.body().decl(codeModel.ref(Schema.class), getVariableName("ElementSchema"), readerSchemaField.invoke("getValueType"));
                    registerSchema(aliasedWriterSchema.getValueType(), schemaVariable);
                }
                processMap(schemaVariable, result, null, aliasedWriterSchema, reader, tryDeserializeBlock.body(), fieldAction);
            }

            JCatchBlock catchBlock = tryDeserializeBlock._catch(codeModel.ref(Throwable.class));
            JVar exceptionVar = catchBlock.param("e");
            catchBlock.body()._throw(JExpr._new(codeModel.ref(IOException.class)).arg(exceptionVar));

            deserializeMethod._throws(codeModel.ref(IOException.class));
            deserializeMethod.param(Decoder.class, DECODER);

            deserializeMethod.body()._return(result);

            Class<FastDeserializer<T>> clazz = compileClass(className);
            return clazz.getConstructor(Schema.class).newInstance(reader);
        } catch (JClassAlreadyExistsException e) {
            throw new FastDeserializerGeneratorException("Class: " + className + " already exists");
        } catch (Exception e) {
            throw new FastDeserializerGeneratorException(e);
        }
    }

    private void processRecord(JVar schemaVariable, JVar containerVariable, final Schema recordSchema, final Schema readerRecord, JBlock body, FieldAction recordAction) {

        ListIterator<Symbol> actionIterator = actionIterator(recordAction);

        if (!doesNotContainMethod(recordSchema, recordAction.getShouldRead())) {
            if (containerVariable != null) {
                body.assign(containerVariable,
                        JExpr.invoke(getMethod(recordSchema, recordAction.getShouldRead())).arg(JExpr.direct(DECODER)));

            } else {
                body.invoke(getMethod(recordSchema, recordAction.getShouldRead())).arg(JExpr.direct(DECODER));
            }

            // seek through actionIterator
            for (Schema.Field field : recordSchema.getFields()) {
                FieldAction action = seekFieldAction(recordAction.getShouldRead(), field, actionIterator);

                if (action.getSymbol() == END_SYMBOL) {
                    break;
                }
            }
            if (!recordAction.getShouldRead()) {
                return;
            }
            // seek through actionIterator also for default values
            Set<String> fieldNamesSet = recordSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
            for (Schema.Field readerField : readerRecord.getFields()) {
                if (!fieldNamesSet.contains(readerField.name())) {
                    forwardToExpectedDefault(actionIterator);
                    seekFieldAction(true, readerField, actionIterator);
                }
            }
            return;
        } else {
            JMethod method = createMethod(recordSchema, recordAction.getShouldRead());
            method._throws(Throwable.class);

            if (containerVariable != null) {
                body.assign(containerVariable,
                        JExpr.invoke(getMethod(recordSchema, recordAction.getShouldRead())).arg(JExpr.direct(DECODER)));

            } else {
                body.invoke(getMethod(recordSchema, recordAction.getShouldRead())).arg(JExpr.direct(DECODER));
            }

            body = method.body();
        }

        JVar result = null;
        if (recordAction.getShouldRead()) {
            JClass recordClass = schemaAnalyzer.classFromSchema(recordSchema);
            JExpression recordInitializationExpr;

            if (useGenericTypes) {
                recordInitializationExpr = JExpr._new(codeModel.ref(GenericData.Record.class)).arg(schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(recordSchema))));
            } else {
                recordInitializationExpr = JExpr._new(codeModel.ref(recordSchema.getFullName()));
            }
            result = body.decl(recordClass, "result", recordInitializationExpr);
        }

        for (Schema.Field field : recordSchema.getFields()) {

            FieldAction action = seekFieldAction(recordAction.getShouldRead(), field, actionIterator);

            if (action.getSymbol() == END_SYMBOL) {
                break;
            }

            JVar containerVar = null;
            JVar schemaVar = null;
            Schema readerFieldSchema = null;
            Schema.Field readerField = null;

            if (action.getShouldRead()) {
                containerVar = declareContainerVariableForSchemaInBlock(field.name(), field.schema(), body);
                schemaVar = declareSchemaVariableForRecordField(field.name(), field.schema(), schemaVariable);
                readerField = readerRecord.getField(field.name());
                readerFieldSchema = readerField.schema();
            }

            if (Schema.Type.UNION.equals(action.getType())) {
                processUnion(schemaVar, result, field.name(), recordSchema, field.schema(), readerFieldSchema,
                        readerField, body, action);
                continue;
            }

            if (Schema.Type.RECORD.equals(action.getType())) {
                processRecord(schemaVar, containerVar, field.schema(), readerFieldSchema, body, action);
            } else if (Schema.Type.ARRAY.equals(action.getType())) {
                processArray(schemaVar, containerVar, field.name(), field.schema(), readerFieldSchema, body, action);
            } else if (Schema.Type.MAP.equals(action.getType())) {
                processMap(schemaVar, containerVar, field.name(), field.schema(), readerFieldSchema, body, action);
            } else if (Schema.Type.ENUM.equals(action.getType())) {
                processEnum(result, recordSchema, field.schema(), readerField, body, action);
                continue;
            } else if (Schema.Type.FIXED.equals(action.getType())) {
                processFixed(result, recordSchema, field.schema(), readerField, body, action);
                continue;
            } else {
                processPrimitive(result, recordSchema, field.schema(), readerField, body, action);
                continue;
            }

            if (action.getShouldRead()) {
                body.invoke(result, "put").arg(JExpr.lit(readerField.pos())).arg(containerVar);
            }
        }

        // Handle default values
        if (recordAction.getShouldRead()) {
            Set<String> fieldNamesSet = recordSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
            for (Schema.Field readerField : readerRecord.getFields()) {
                if (!fieldNamesSet.contains(readerField.name())) {
                    forwardToExpectedDefault(actionIterator);
                    seekFieldAction(true, readerField, actionIterator);
                    JVar schemaVar = null;
                    if (useGenericTypes) {
                        schemaVar = declareSchemaVariableForRecordField(readerField.name(), readerField.schema(),
                                schemaVariable);
                    }
                    JExpression value = parseDefaultValue(readerField.schema(), readerField.defaultValue(), body,
                            schemaVar, readerField.name());
                    body.invoke(result, "put").arg(JExpr.lit(readerField.pos())).arg(value);
                }
            }
        }

        if (recordAction.getShouldRead()) {
            body._return(result);
        }
    }

    private JExpression parseDefaultValue(Schema schema, JsonNode defaultValue, JBlock body, JVar schemaVariable,
            String fieldName) {
        Schema.Type schemaType = schema.getType();
        // The default value of union is of the first defined type
        if (schemaType == Schema.Type.UNION) {
            schema = schema.getTypes().get(0);
            schemaType = schema.getType();
            schemaVariable = declareSchemaVariableForUnion(fieldName, schema, schemaVariable, 0);
        }


        if (Schema.Type.RECORD.equals(schemaType)) {
            JClass defaultValueClass = schemaAnalyzer.classFromSchema(schema);
            JInvocation recordInitializationExpression = JExpr._new(defaultValueClass);
            if (useGenericTypes) {
                recordInitializationExpression = recordInitializationExpression.arg(schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema))));
            }
            JVar recordVar = body.decl(defaultValueClass, getVariableName("default" + schema.getName()), recordInitializationExpression);

            for (Iterator<Map.Entry<String, JsonNode>> it = defaultValue.getFields(); it.hasNext();) {
                Map.Entry<String, JsonNode> subFieldEntry = it.next();
                String subFieldName = subFieldEntry.getKey();
                Schema.Field subField = schema.getField(subFieldName);
                JsonNode value = subFieldEntry.getValue();

                int fieldNumber = subField.pos();

                JVar schemaVar = declareSchemaVariableForRecordField(subField.name(), subField.schema(),
                        schemaVariable);
                JExpression fieldValue = parseDefaultValue(subField.schema(), value, body, schemaVar, subField.name());
                body.invoke(recordVar, "put").arg(JExpr.lit(fieldNumber)).arg(fieldValue);
            }
            return recordVar;

        } else if (Schema.Type.ARRAY.equals(schemaType)) {
            JClass defaultValueClass = schemaAnalyzer.classFromSchema(schema, true);
            Schema elementSchema = schema.getElementType();
            JVar elementSchemaVariable = declareSchemaVariableForCollectionElement(fieldName + "Element", elementSchema, schemaVariable);
            JVar arrayVar;
            JInvocation arrayVarInitialization = JExpr._new(defaultValueClass);

            if (useGenericTypes) {
                JInvocation getSchema = schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema)));
                int elementCount = defaultValue.size();
                arrayVarInitialization = arrayVarInitialization.arg(JExpr.lit(elementCount)).arg(getSchema);

            }

            arrayVar = body.decl(defaultValueClass, getVariableName("defaultArray"), arrayVarInitialization);

            for (JsonNode arrayEntryValue : defaultValue) {
                JExpression fieldValue = parseDefaultValue(elementSchema, arrayEntryValue, body, elementSchemaVariable,
                        "arrayValue");
                body.invoke(arrayVar, "add").arg(fieldValue);
            }
            return arrayVar;

        } else if (Schema.Type.MAP.equals(schemaType)) {
            JClass defaultValueClass = schemaAnalyzer.classFromSchema(schema, true);
            JVar mapVar = body.decl(defaultValueClass,
                    getVariableName("defaultMap"),
                    JExpr._new(codeModel.ref(HashMap.class)
                            .narrow(schemaAnalyzer.keyClassFromMapSchema(schema))
                            .narrow(schemaAnalyzer.elementClassFromMapSchema(schema))));

            JVar elementSchemaVariable = declareSchemaVariableForCollectionElement(fieldName + "Value",
                    schema.getValueType(), schemaVariable);
            for (Iterator<Map.Entry<String, JsonNode>> it = defaultValue.getFields(); it.hasNext();) {
                Map.Entry<String, JsonNode> mapEntry = it.next();
                JExpression fieldValue = parseDefaultValue(schema.getValueType(), mapEntry.getValue(), body,
                        elementSchemaVariable, "mapElement");
                body.invoke(mapVar, "put").arg(mapEntry.getKey()).arg(fieldValue);
            }
            return mapVar;

        } else if (Schema.Type.ENUM.equals(schemaType)) {
            String value = defaultValue.getTextValue();
            if (!useGenericTypes) {
                return codeModel.ref(schema.getFullName()).staticInvoke("valueOf").arg(value);
            } else {
                JInvocation getSchema = schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema)));
                return JExpr._new(codeModel.ref(GenericData.EnumSymbol.class)).arg(getSchema).arg(value);
            }

        } else if (Schema.Type.FIXED.equals(schemaType)) {
            String value = defaultValue.getTextValue();
            JArray bytesArray = JExpr.newArray(codeModel.BYTE);
            for (char b : value.toCharArray()) {
                bytesArray.add(JExpr.lit((byte) b));
            }
            if (!useGenericTypes) {
                return JExpr._new(codeModel.ref(schema.getFullName())).arg(bytesArray);
            } else {
                JInvocation getSchema = schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema)));
                return JExpr._new(codeModel.ref(GenericData.Fixed.class)).arg(getSchema).arg(bytesArray);
            }

        } else if (Schema.Type.BYTES.equals(schemaType)) {
            String value = defaultValue.getTextValue();
            JArray bytesArray = JExpr.newArray(codeModel.BYTE);
            for (byte b : value.getBytes()) {
                bytesArray.add(JExpr.lit(b));
            }
            return codeModel.ref(ByteBuffer.class).staticInvoke("wrap").arg(bytesArray);

        } else if (Schema.Type.INT.equals(schemaType)) {
            int value = defaultValue.getIntValue();
            return JExpr.lit(value);
        } else if (Schema.Type.LONG.equals(schemaType)) {
            long value = defaultValue.getLongValue();
            return JExpr.lit(value);
        } else if (Schema.Type.DOUBLE.equals(schemaType)) {
            double value = defaultValue.getDoubleValue();
            return JExpr.lit(value);
        } else if (Schema.Type.FLOAT.equals(schemaType)) {
            float value = (float) defaultValue.getDoubleValue();
            return JExpr.lit(value);
        } else if (Schema.Type.STRING.equals(schemaType)) {
            String value = defaultValue.getTextValue();
            return JExpr.lit(value);
        } else if (Schema.Type.BOOLEAN.equals(schemaType)) {
            Boolean value = defaultValue.getBooleanValue();
            return JExpr.lit(value);
        }
        return JExpr._null();
    }

    private void processUnion(JVar schemaVariable, JVar containerVariable, final String name,
            final Schema containerSchema,
            final Schema unionSchema, final Schema readerUnionSchema, JBlock body, FieldAction action) {
        processUnion(schemaVariable, containerVariable, name, containerSchema, unionSchema, readerUnionSchema, null,
                body, action);
    }

    private void processUnion(JVar schemaVariable, JVar containerVariable, final String name,
            final Schema containerSchema,
            final Schema unionSchema, final Schema readerUnionSchema, final Schema.Field readerUnionField, JBlock body,
            FieldAction action) {

        JVar key = null;
        if (containerSchema.getType().equals(Schema.Type.MAP)) {
            key = body.decl(codeModel.ref(String.class), getVariableName("key"),
                    JExpr.direct(DECODER + ".readString()"));
        }

        JVar unionIndex = body
                .decl(codeModel.INT, getVariableName("unionIndex"), JExpr.direct(DECODER + ".readIndex()"));

        for (int i = 0; i < unionSchema.getTypes().size(); i++) {
            if (Schema.Type.NULL.equals(unionSchema.getTypes().get(i).getType())) {
                body._if(unionIndex.eq(JExpr.lit(i)))._then().directStatement(DECODER + ".readNull();");
                continue;
            }

            Schema unionFieldSchema = unionSchema.getTypes().get(i);
            Schema readerUnionFieldSchema = null;
            FieldAction unionAction;

            if (action.getShouldRead()) {

                readerUnionFieldSchema = readerUnionSchema.getTypes().get(i);

                Symbol.Alternative alternative = null;
                if (action.getSymbol() instanceof Symbol.Alternative) {
                    alternative = (Symbol.Alternative) action.getSymbol();
                } else if (action.getSymbol().production != null) {
                    for (Symbol symbol : action.getSymbol().production) {
                        if (symbol instanceof Symbol.Alternative) {
                            alternative = (Symbol.Alternative) symbol;
                            break;
                        }
                    }

                }

                if (alternative == null) {
                    throw new FastDeserializerGeneratorException("Unable to determine action for field: " + name);
                }

                Symbol.UnionAdjustAction unionAdjustAction = (Symbol.UnionAdjustAction) alternative.symbols[i].production[0];

                unionAction = FieldAction.fromValues(unionFieldSchema.getType(), action.getShouldRead(),
                        unionAdjustAction.symToParse);
            } else {
                unionAction = FieldAction.fromValues(unionFieldSchema.getType(), false, EMPTY_SYMBOL);
            }

            JBlock block = body._if(unionIndex.eq(JExpr.lit(i)))._then();

            JVar schemaVar = null;
            JVar containerVar = null;

            if (unionAction.getShouldRead()) {
                containerVar = declareContainerVariableForSchemaInBlock(name, unionFieldSchema, block);
                schemaVar = declareSchemaVariableForUnion(name, unionFieldSchema, schemaVariable, i);
            }

            if (Schema.Type.RECORD.equals(unionAction.getType())) {
                processRecord(schemaVar, containerVar, unionFieldSchema, readerUnionFieldSchema, block, unionAction);
            } else if (Schema.Type.ARRAY.equals(unionAction.getType())) {
                processArray(schemaVar, containerVar, name, unionFieldSchema, readerUnionFieldSchema, block,
                        unionAction);
            } else if (Schema.Type.MAP.equals(unionAction.getType())) {
                processMap(schemaVar, containerVar, name, unionFieldSchema, readerUnionFieldSchema, block, unionAction);
            } else if (Schema.Type.ENUM.equals(unionAction.getType())) {
                processEnum(containerVariable, key, containerSchema, unionFieldSchema, readerUnionField, block,
                        unionAction);
                // intentional continue
                continue;
            } else if (Schema.Type.FIXED.equals(unionAction.getType())) {
                processFixed(containerVariable, key, containerSchema, unionFieldSchema, readerUnionField, block,
                        unionAction);
                // intentional continue
                continue;
            } else {
                processPrimitive(containerVariable, key, containerSchema, unionFieldSchema,
                        readerUnionField, block, unionAction);
                // intentional continue
                continue;
            }

            if (unionAction.getShouldRead()) {
                if (Schema.Type.RECORD.equals(containerSchema.getType())) {
                    block.invoke(containerVariable, "put").arg(JExpr.lit(readerUnionField.pos()))
                            .arg(containerVar);
                } else if (Schema.Type.ARRAY.equals(containerSchema.getType())) {
                    block.invoke(containerVariable, "add").arg(containerVar);
                } else if (Schema.Type.MAP.equals(containerSchema.getType())) {
                    block.invoke(containerVariable, "put").arg(key).arg(containerVar);
                }
            }
        }
    }

    private void processArray(JVar schemaVariable, JVar containerVariable, final String name, final Schema arraySchema,
            final Schema readerArraySchema, JBlock body, FieldAction action) {
        if (action.getShouldRead()) {
            Symbol valuesActionSymbol = null;
            for (Symbol symbol : action.getSymbol().production) {
                if (Symbol.Kind.REPEATER.equals(symbol.kind)
                        && "array-end".equals(getSymbolPrintName(((Symbol.Repeater) symbol).end))) {
                    valuesActionSymbol = symbol;
                    break;
                }
            }

            if (valuesActionSymbol == null) {
                throw new FastDeserializerGeneratorException("Unable to determine action for array: " + name);
            }

            action = FieldAction.fromValues(arraySchema.getElementType().getType(), action.getShouldRead(),
                    valuesActionSymbol);
        } else {
            action = FieldAction.fromValues(arraySchema.getElementType().getType(), false, EMPTY_SYMBOL);
        }

        JVar chunklen = body.decl(codeModel.LONG, getVariableName("chunklen"),
                JExpr.direct(DECODER + ".readArrayStart()"));

        JConditional conditional = body._if(chunklen.gt(JExpr.lit(0)));
        JBlock ifBlock = conditional._then();

        JClass arrayClass = schemaAnalyzer.classFromSchema(arraySchema, true);

        if (action.getShouldRead()) {
            if (useGenericTypes) {
                ifBlock.assign(containerVariable,
                        JExpr._new(arrayClass).arg(JExpr.cast(codeModel.INT, chunklen))
                                .arg(schemaMapField.invoke("get")
                                        .arg(JExpr.lit(getSchemaId(arraySchema)))));
            } else {
                ifBlock.assign(containerVariable, JExpr._new(arrayClass));
            }
            JBlock elseBlock = conditional._else();
            if (useGenericTypes) {
                elseBlock
                        .assign(containerVariable,
                                JExpr._new(arrayClass)
                                        .arg(JExpr.lit(0))
                                        .arg(schemaMapField.invoke("get").arg(
                                                JExpr.lit(getSchemaId(arraySchema)))));
            } else {
                elseBlock.assign(containerVariable, codeModel.ref(Collections.class).staticInvoke("emptyList"));
            }
        }

        JDoLoop doLoop = ifBlock._do(chunklen.gt(JExpr.lit(0)));
        JForLoop forLoop = doLoop.body()._for();
        JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
        forLoop.test(counter.lt(chunklen));
        forLoop.update(counter.incr());
        JBlock forBody = forLoop.body();

        JVar containerVar = null;
        Schema readerArrayElementSchema = null;

        if (action.getShouldRead()) {
            containerVar = declareContainerVariableForSchemaInBlock(name, arraySchema.getElementType(), forBody);
            readerArrayElementSchema = readerArraySchema.getElementType();
        }

        if (Schema.Type.UNION.equals(action.getType())) {
            processUnion(schemaVariable, containerVariable, name, arraySchema, arraySchema.getElementType(),
                    readerArrayElementSchema, forBody, action);

            doLoop.body().assign(chunklen, JExpr.direct(DECODER + ".arrayNext()"));
            // intentional return
            return;
        }

        if (Schema.Type.RECORD.equals(action.getType())) {
            processRecord(schemaVariable, containerVar, arraySchema.getElementType(), readerArrayElementSchema,
                    forBody, action);
        } else if (Schema.Type.ARRAY.equals(action.getType())) {
            if (action.getShouldRead()) {
                schemaVariable = declareSchemaVariableForCollectionElement(name, arraySchema.getElementType(),
                        schemaVariable);
            }
            processArray(schemaVariable, containerVar, name, arraySchema.getElementType(), readerArrayElementSchema,
                    forBody, action);
        } else if (Schema.Type.MAP.equals(action.getType())) {
            if (action.getShouldRead()) {
                schemaVariable = declareSchemaVariableForCollectionElement(name, arraySchema.getElementType(),
                        schemaVariable);
            }
            processMap(schemaVariable, containerVar, name, arraySchema.getElementType(), readerArrayElementSchema,
                    forBody, action);
        } else if (Schema.Type.ENUM.equals(action.getType())) {
            processEnum(containerVariable, arraySchema, arraySchema.getElementType(), forBody, action);

            doLoop.body().assign(chunklen, JExpr.direct(DECODER + ".arrayNext()"));
            // intentional return
            return;
        } else if (Schema.Type.FIXED.equals(action.getType())) {
            processFixed(containerVariable, arraySchema, arraySchema.getElementType(), forBody, action);

            doLoop.body().assign(chunklen, JExpr.direct(DECODER + ".arrayNext()"));
            // intentional return
            return;
        } else {
            processPrimitive(containerVariable, arraySchema, arraySchema.getElementType(), forBody, action);

            doLoop.body().assign(chunklen, JExpr.direct(DECODER + ".arrayNext()"));
            // intentional return
            return;
        }

        if (action.getShouldRead()) {
            forBody.invoke(containerVariable, "add").arg(containerVar);
        }

        doLoop.body().assign(chunklen, JExpr.direct(DECODER + ".arrayNext()"));
    }

    private void processMap(JVar schemaVariable, JVar containerVariable, final String name, final Schema mapSchema,
            final Schema readerMapSchema, JBlock body, FieldAction action) {

        if (action.getShouldRead()) {
            Symbol valuesActionSymbol = null;
            for (Symbol symbol : action.getSymbol().production) {
                if (Symbol.Kind.REPEATER.equals(symbol.kind)
                        && "map-end".equals(getSymbolPrintName(((Symbol.Repeater) symbol).end))) {
                    valuesActionSymbol = symbol;
                    break;
                }
            }

            if (valuesActionSymbol == null) {
                throw new FastDeserializerGeneratorException("unable to determine action for map: " + name);
            }

            action = FieldAction.fromValues(mapSchema.getValueType().getType(), action.getShouldRead(),
                    valuesActionSymbol);
        } else {
            action = FieldAction.fromValues(mapSchema.getValueType().getType(), false, EMPTY_SYMBOL);
        }

        JVar chunklen = body.decl(codeModel.LONG, getVariableName("chunklen"),
                JExpr.direct(DECODER + ".readMapStart()"));

        JConditional conditional = body._if(chunklen.gt(JExpr.lit(0)));
        JBlock ifBlock = conditional._then();

        if (action.getShouldRead()) {
            ifBlock.assign(
                    containerVariable,
                    JExpr._new(codeModel.ref(HashMap.class)
                            .narrow(schemaAnalyzer.keyClassFromMapSchema(mapSchema),
                                    schemaAnalyzer.elementClassFromMapSchema(mapSchema))));
            JBlock elseBlock = conditional._else();
            elseBlock.assign(containerVariable, codeModel.ref(Collections.class).staticInvoke("emptyMap"));
        }

        JDoLoop doLoop = ifBlock._do(chunklen.gt(JExpr.lit(0)));
        JForLoop forLoop = doLoop.body()._for();
        JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
        forLoop.test(counter.lt(chunklen));
        forLoop.update(counter.incr());
        JBlock forBody = forLoop.body();

        JVar containerVar = null;
        Schema readerMapValueSchema = null;

        if (action.getShouldRead()) {
            containerVar = declareContainerVariableForSchemaInBlock(name, mapSchema.getValueType(), forBody);
            readerMapValueSchema = readerMapSchema.getValueType();
        }

        if (Schema.Type.UNION.equals(action.getType())) {
            processUnion(schemaVariable, containerVariable, name, mapSchema, mapSchema.getValueType(),
                    readerMapValueSchema, forBody, action);

            doLoop.body().assign(chunklen, JExpr.direct(DECODER + ".mapNext()"));
            // intentional return
            return;
        }

        JClass keyClass = schemaAnalyzer.keyClassFromMapSchema(mapSchema);
        JExpression keyValueExpression = JExpr.direct(DECODER + ".readString()");
        if (!keyClass.name().equals(String.class.getName())) {
            keyValueExpression = JExpr._new(keyClass).arg(keyValueExpression);
        }

        JVar key = forBody.decl(keyClass, getVariableName("key"), keyValueExpression);

        if (Schema.Type.RECORD.equals(action.getType())) {
            processRecord(schemaVariable, containerVar, mapSchema.getValueType(), readerMapValueSchema, forBody,
                    action);
        } else if (Schema.Type.ARRAY.equals(action.getType())) {
            if (action.getShouldRead()) {
                schemaVariable = declareSchemaVariableForCollectionElement(name, mapSchema.getValueType(),
                        schemaVariable);
            }

            processArray(schemaVariable, containerVar, name, mapSchema.getValueType(), readerMapValueSchema, forBody,
                    action);
        } else if (Schema.Type.MAP.equals(action.getType())) {
            if (action.getShouldRead()) {
                schemaVariable = declareSchemaVariableForCollectionElement(name, mapSchema.getValueType(),
                        schemaVariable);
            }

            processMap(schemaVariable, containerVar, name, mapSchema.getValueType(), readerMapValueSchema, forBody,
                    action);
        } else if (Schema.Type.ENUM.equals(action.getType())) {
            processEnum(containerVariable, key, mapSchema, mapSchema.getValueType(), forBody, action);

            doLoop.body().assign(chunklen, JExpr.direct(DECODER + ".mapNext()"));
            // intentional return
            return;
        } else if (Schema.Type.FIXED.equals(action.getType())) {
            processFixed(containerVariable, key, mapSchema, mapSchema.getValueType(), forBody, action);

            doLoop.body().assign(chunklen, JExpr.direct(DECODER + ".mapNext()"));
            // intentional return
            return;
        } else {
            processPrimitive(containerVariable, key, mapSchema, mapSchema.getValueType(), forBody, action);

            doLoop.body().assign(chunklen, JExpr.direct(DECODER + ".mapNext()"));
            // intentional return
            return;
        }

        if (action.getShouldRead()) {
            forBody.invoke(containerVariable, "put").arg(key).arg(containerVar);
        }

        doLoop.body().assign(chunklen, JExpr.direct(DECODER + ".mapNext()"));
    }

    private void processFixed(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            final Schema schema,
            JBlock body, FieldAction action) {
        processFixed(containerVariable, keyVariable, containerSchema, schema, null, body, action);
    }

    private void processFixed(JVar containerVariable, final Schema containerSchema, final Schema schema,
            final Schema.Field readerField, JBlock body, FieldAction action) {
        processFixed(containerVariable, null, containerSchema, schema, readerField, body, action);
    }

    private void processFixed(JVar containerVariable, final Schema containerSchema, final Schema schema, JBlock body,
            FieldAction action) {
        processFixed(containerVariable, null, containerSchema, schema, null, body, action);
    }

    private void processFixed(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            final Schema schema,
            final Schema.Field readerField, JBlock body, FieldAction action) {
        if (action.getShouldRead()) {
            JInvocation getSchema = null;
            if (useGenericTypes) {
                getSchema = schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema)));
            }

            JVar fixedBuffer = body.decl(codeModel.ref(byte[].class), getVariableName(schema.getName()))
                    .init(JExpr.direct(" new byte[" + schema.getFixedSize() + "]"));

            body.directStatement(DECODER + ".readFixed(" + fixedBuffer.name() + ");");
            JExpression fixed = useGenericTypes ? JExpr
                    ._new(codeModel.ref(GenericData.Fixed.class))
                    .arg(getSchema).arg(fixedBuffer)
                    : JExpr._new(codeModel.ref(schema.getFullName())).arg(fixedBuffer);

            if (Schema.Type.RECORD.equals(containerSchema.getType())) {
                body.invoke(containerVariable, "put").arg(JExpr.lit(readerField.pos()))
                        .arg(fixed);
            } else if (Schema.Type.ARRAY.equals(containerSchema.getType())) {
                body.invoke(containerVariable, "add").arg(fixed);
            } else if (Schema.Type.MAP.equals(containerSchema.getType())) {
                body.invoke(containerVariable, "put").arg(keyVariable).arg(fixed);
            }
        } else {
            body.directStatement(DECODER + ".skipFixed(" + schema.getFixedSize() + ");");
        }
    }

    private void processEnum(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            final Schema schema,
            JBlock body, FieldAction action) {
        processEnum(containerVariable, keyVariable, containerSchema, schema, null, body, action);
    }

    private void processEnum(JVar containerVariable, final Schema containerSchema, final Schema schema,
            final Schema.Field readerField, JBlock body, FieldAction action) {
        processEnum(containerVariable, null, containerSchema, schema, readerField, body, action);
    }

    private void processEnum(JVar containerVariable, final Schema containerSchema, final Schema schema, JBlock body,
            FieldAction action) {
        processEnum(containerVariable, null, containerSchema, schema, null, body, action);
    }

    private void processEnum(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            final Schema schema,
            final Schema.Field readerField, JBlock body, FieldAction action) {
        if (action.getShouldRead()) {
            JInvocation getSchema = null;
            if (useGenericTypes) {
                getSchema = schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema)));
            }

            Symbol.EnumAdjustAction enumAdjustAction = null;
            if (action.getSymbol() instanceof Symbol.EnumAdjustAction) {
                enumAdjustAction = (Symbol.EnumAdjustAction) action.getSymbol();
            } else {
                for (Symbol symbol : action.getSymbol().production) {
                    if (symbol instanceof Symbol.EnumAdjustAction) {
                        enumAdjustAction = (Symbol.EnumAdjustAction) symbol;
                    }
                }
            }

            boolean enumOrderCorrect = true;
            for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
                Object adjustment = enumAdjustAction.adjustments[i];
                if (adjustment instanceof String) {
                    throw new FastDeserializerGeneratorException(schema.getName()
                            + " enum label impossible to deserialize: "
                            + adjustment.toString());
                } else if (!adjustment.equals(i)) {
                    enumOrderCorrect = false;
                }
            }

            JExpression newEnum;
            if (enumOrderCorrect) {
                newEnum = useGenericTypes ? JExpr
                        ._new(codeModel.ref(GenericData.EnumSymbol.class))
                        .arg(getSchema)
                        .arg(getSchema.invoke("getEnumSymbols").invoke("get")
                                .arg(JExpr.direct(DECODER + ".readEnum()")))
                        : codeModel.ref(schema.getFullName()).staticInvoke("values")
                                .component(JExpr.direct(DECODER + ".readEnum()"));
            } else {
                JVar enumIndex = body.decl(codeModel.INT, getVariableName("enumIndex"),
                        JExpr.direct(DECODER + ".readEnum()"));
                newEnum = useGenericTypes ? body.decl(codeModel.ref(GenericData.EnumSymbol.class),
                        getVariableName("enumValue"), JExpr._null())
                        : body.decl(codeModel.ref(schema.getFullName()),
                                getVariableName("enumValue"), JExpr._null());

                for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
                    if (useGenericTypes) {
                        body._if(enumIndex.eq(JExpr.lit(i)))
                                ._then()
                                .assign((JVar) newEnum,
                                        JExpr._new(codeModel.ref(GenericData.EnumSymbol.class))
                                                .arg(getSchema)
                                                .arg(getSchema.invoke("getEnumSymbols").invoke("get")
                                                        .arg(JExpr.lit((Integer) enumAdjustAction.adjustments[i]))));
                    } else {
                        body._if(enumIndex.eq(JExpr.lit(i)))
                                ._then()
                                .assign((JVar) newEnum,
                                        codeModel.ref(schema.getFullName()).staticInvoke("values")
                                                .component(JExpr.lit((Integer) enumAdjustAction.adjustments[i])));
                    }
                }
            }

            if (Schema.Type.RECORD.equals(containerSchema.getType())) {
                body.invoke(containerVariable, "put").arg(JExpr.lit(readerField.pos()))
                        .arg(newEnum);
            } else if (Schema.Type.ARRAY.equals(containerSchema.getType())) {
                body.invoke(containerVariable, "add").arg(newEnum);
            } else if (Schema.Type.MAP.equals(containerSchema.getType())) {
                body.invoke(containerVariable, "put").arg(keyVariable).arg(newEnum);
            }
        } else {
            body.directStatement(DECODER + ".readEnum();");
        }

    }

    private void processPrimitive(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            final Schema fieldSchema, JBlock body, FieldAction action) {
        processPrimitive(containerVariable, keyVariable, containerSchema, fieldSchema, null, body, action);
    }

    private void processPrimitive(JVar containerVariable, final Schema containerSchema,
            final Schema fieldSchema, final Schema.Field readerField, JBlock body, FieldAction action) {
        processPrimitive(containerVariable, null, containerSchema, fieldSchema, readerField, body, action);
    }

    private void processPrimitive(JVar containerVariable, final Schema containerSchema,
            final Schema fieldSchema, JBlock body, FieldAction action) {
        processPrimitive(containerVariable, null, containerSchema, fieldSchema, null, body, action);
    }

    private void processPrimitive(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            final Schema fieldSchema, final Schema.Field readerField, JBlock body, FieldAction action) {
        String readFunction = null;

        if (Schema.Type.BOOLEAN.equals(fieldSchema.getType())) {
            readFunction = "readBoolean()";
        } else if (Schema.Type.INT.equals(fieldSchema.getType())) {
            readFunction = "readInt()";
        } else if (Schema.Type.LONG.equals(fieldSchema.getType())) {
            readFunction = "readLong()";
        } else if (Schema.Type.STRING.equals(fieldSchema.getType())) {
            readFunction = action.getShouldRead() ? "readString()"
                    : "skipString()";
        } else if (Schema.Type.DOUBLE.equals(fieldSchema.getType())) {
            readFunction = "readDouble()";
        } else if (Schema.Type.FLOAT.equals(fieldSchema.getType())) {
            readFunction = "readFloat()";
        } else if (Schema.Type.BYTES.equals(fieldSchema.getType())) {
            readFunction = "readBytes(null)";
        }

        if (readFunction == null) {
            throw new FastDeserializerGeneratorException(
                    "Unsupported primitive schema of type: " + fieldSchema.getType());
        }

        JExpression primitiveValueExpression = JExpr.direct("decoder." + readFunction);
        if (action.getShouldRead()) {
            String valueJavaClassName = fieldSchema.getProp("java-class");
            if (valueJavaClassName != null) {
                try {
                    primitiveValueExpression = JExpr._new(codeModel.ref(Class.forName(valueJavaClassName))).arg(primitiveValueExpression);
                } catch (ClassNotFoundException e) {
                    throw new FastDeserializerGeneratorException("Unknown value java class: " + valueJavaClassName);
                }
            }

            if (Schema.Type.RECORD.equals(containerSchema.getType())) {
                body.invoke(containerVariable, "put").arg(JExpr.lit(readerField.pos())).arg(primitiveValueExpression);
            } else if (Schema.Type.ARRAY.equals(containerSchema.getType())) {
                body.invoke(containerVariable, "add").arg(primitiveValueExpression);
            } else if (Schema.Type.MAP.equals(containerSchema.getType())) {
                body.invoke(containerVariable, "put").arg(keyVariable).arg(primitiveValueExpression);
            }
        } else {
            body.directStatement(DECODER + "." + readFunction + ";");
        }
    }

    private JVar declareContainerVariableForSchemaInBlock(final String name, final Schema schema, JBlock block) {
        if (Schema.Type.ARRAY.equals(schema.getType())
                || Schema.Type.MAP.equals(schema.getType())
                || Schema.Type.RECORD.equals(schema.getType())) {
            return block.decl(schemaAnalyzer.classFromSchema(schema), getVariableName(name), JExpr._null());
        }

        return null;
    }

    private JVar declareSchemaVariableForUnion(final String name, final Schema unionFieldSchema, JVar schemaVar,
            int paramNumber) {
        if (!useGenericTypes) {
            return null;
        }

        if (Schema.Type.RECORD.equals(unionFieldSchema.getType())
                || Schema.Type.ENUM.equals(unionFieldSchema.getType())) {
            schemaVar = schemaMapMethod.body().decl(
                    codeModel.ref(Schema.class),
                    getVariableName(name + "Schema"),
                    schemaVar.invoke("getTypes").invoke("get")
                            .arg(JExpr.lit(paramNumber)));

            registerSchema(unionFieldSchema, schemaVar);
        } else if (Schema.Type.ARRAY.equals(unionFieldSchema.getType())) {
            schemaVar = schemaMapMethod.body().decl(
                    codeModel.ref(Schema.class),
                    getVariableName(name + "Schema"),
                    schemaVar
                            .invoke("getTypes")
                            .invoke("get")
                            .arg(JExpr.lit(paramNumber)));

            registerSchema(unionFieldSchema, schemaVar);

            schemaVar = schemaMapMethod.body().decl(
                    codeModel.ref(Schema.class),
                    getVariableName(name + "Schema"),
                    schemaVar.invoke("getElementType"));

            registerSchema(unionFieldSchema.getElementType(), schemaVar);
        } else if (Schema.Type.MAP.equals(unionFieldSchema.getType())) {
            schemaVar = schemaMapMethod.body().decl(
                    codeModel.ref(Schema.class),
                    getVariableName(name + "Schema"),
                    schemaVar
                            .invoke("getTypes")
                            .invoke("get")
                            .arg(JExpr.lit(paramNumber))
                            .invoke("getValueType"));
            registerSchema(unionFieldSchema.getValueType(), schemaVar);
        }

        return schemaVar;
    }

    private JVar declareSchemaVariableForCollectionElement(final String name, final Schema schema, JVar schemaVar) {
        if (!useGenericTypes) {
            return null;
        }

        if (Schema.Type.ARRAY.equals(schema.getType())) {
            schemaVar = schemaMapMethod.body().decl(codeModel.ref(Schema.class),
                    getVariableName(name + "ArraySchema"),
                    schemaVar.invoke("getElementType"));

            registerSchema(schema.getElementType(), schemaVar);
        } else if (Schema.Type.MAP.equals(schema.getType())) {
            schemaVar = schemaMapMethod.body().decl(codeModel.ref(Schema.class),
                    getVariableName(name + "MapSchema"),
                    schemaVar.invoke("getValueType"));

            registerSchema(schema.getValueType(), schemaVar);
        }

        return schemaVar;
    }

    private JVar declareSchemaVariableForRecordField(final String name, final Schema schema, JVar schemaVar) {
        if (!useGenericTypes) {
            return null;
        }

        if (Schema.Type.RECORD.equals(schema.getType()) || Schema.Type.ENUM.equals(schema.getType())) {
            schemaVar = schemaMapMethod.body().decl(codeModel.ref(Schema.class), getVariableName(name + "Schema"),
                    schemaVar.invoke("getField").arg(name).invoke("schema"));

            registerSchema(schema, schemaVar);
        } else if (Schema.Type.ARRAY.equals(schema.getType())) {
            schemaVar = schemaMapMethod.body().decl(codeModel.ref(Schema.class), getVariableName(name + "Schema"),
                    schemaVar.invoke("getField").arg(name).invoke("schema"));

            registerSchema(schema, schemaVar);

            schemaVar = schemaMapMethod.body().decl(codeModel.ref(Schema.class), getVariableName(name + "Schema"),
                    schemaVar.invoke("getElementType"));

            registerSchema(schema.getElementType(), schemaVar);
        } else if (Schema.Type.MAP.equals(schema.getType())) {
            schemaVar = schemaMapMethod.body().decl(codeModel.ref(Schema.class), getVariableName(name + "Schema"),
                    schemaVar.invoke("getField").arg(name).invoke("schema").invoke("getValueType"));

            registerSchema(schema.getValueType(), schemaVar);
        } else if (Schema.Type.UNION.equals(schema.getType()) && !schemaAnalyzer.isPrimitiveTypeUnion(schema)) {
            schemaVar = schemaMapMethod.body().decl(codeModel.ref(Schema.class), getVariableName(name + "Schema"),
                    schemaVar.invoke("getField").arg(name).invoke("schema"));
        }
        return schemaVar;
    }

    private boolean doesNotContainMethod(final Schema schema, boolean read) {
        if (read) {
            return Schema.Type.RECORD.equals(schema.getType())
                    && !deserializeMethodMap.containsKey(schema.getFullName());
        }
        return Schema.Type.RECORD.equals(schema.getType()) && !skipMethodMap.containsKey(schema.getFullName());
    }

    private JMethod getMethod(final Schema schema, boolean read) {
        if (Schema.Type.RECORD.equals(schema.getType())) {
            if (!doesNotContainMethod(schema, read)) {
                return read ? deserializeMethodMap.get(schema.getFullName()) : skipMethodMap.get(schema.getFullName());
            }
            throw new FastDeserializerGeneratorException("No method for schema: " + schema.getFullName());
        }
        throw new FastDeserializerGeneratorException("No method for schema type: " + schema.getType());
    }

    private JMethod createMethod(final Schema schema, boolean read) {
        if (Schema.Type.RECORD.equals(schema.getType())) {
            if (doesNotContainMethod(schema, read)) {
                JMethod method;
                if (useGenericTypes) {
                    method = deserializerClass.method(JMod.PUBLIC, read ? codeModel.ref(GenericData.Record.class)
                            : codeModel.VOID,
                            "deserialize" + schema.getName() + nextRandomInt());
                } else {
                    method = deserializerClass.method(JMod.PUBLIC, read ? codeModel.ref(schema.getFullName())
                            : codeModel.VOID,
                            "deserialize" + schema.getName() + nextRandomInt());

                }

                method._throws(IOException.class);

                method.param(Decoder.class, DECODER);

                if (read) {
                    deserializeMethodMap.put(schema.getFullName(), method);
                } else {
                    skipMethodMap.put(schema.getFullName(), method);
                }

                return method;
            } else {
                throw new FastDeserializerGeneratorException("Method already exists for: " + schema.getFullName());
            }
        }
        throw new FastDeserializerGeneratorException("No method for schema type: " + schema.getType());
    }

    private void registerSchema(final Schema schema, JVar schemaVar) {
        if ((Schema.Type.RECORD.equals(schema.getType()) || Schema.Type.ENUM.equals(schema.getType())
                || Schema.Type.ARRAY
                        .equals(schema.getType()))
                && doesNotContainSchema(schema)) {
            schemaMap.put(getSchemaId(schema), schema);

            schemaMapMethod.body().invoke(schemaMapField, "put")
                    .arg(JExpr.lit(getSchemaId(schema))).arg(schemaVar);
        }
    }

    private boolean doesNotContainSchema(final Schema schema) {
        return !schemaMap.containsKey(getSchemaId(schema));
    }

}
