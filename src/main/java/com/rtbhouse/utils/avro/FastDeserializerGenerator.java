package com.rtbhouse.utils.avro;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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

            JVar readerSchemaVar = deserializerClass.field(JMod.PRIVATE | JMod.FINAL, Schema.class, "readerSchema");
            JMethod constructor = deserializerClass.constructor(JMod.PUBLIC);
            JVar constructorParam = constructor.param(Schema.class, "readerSchema");
            constructor.body().assign(JExpr.refthis(readerSchemaVar.name()), constructorParam);

            Schema aliasedWriterSchema = Schema.applyAliases(writer, reader);
            Symbol resolvingGrammar = new ResolvingGrammarGenerator().generate(aliasedWriterSchema, reader);
            FieldAction fieldAction = FieldAction.fromValues(aliasedWriterSchema.getType(), true, resolvingGrammar);

            if (useGenericTypes) {
                schemaMapField = deserializerClass.field(JMod.PRIVATE,
                        codeModel.ref(Map.class).narrow(Integer.class).narrow(Schema.class), "readerSchemaMap");
                schemaMapMethod = deserializerClass.method(JMod.PRIVATE | JMod.FINAL, void.class, "schemaMap");
                constructor.body().invoke(schemaMapMethod);
                schemaMapMethod.body().assign(schemaMapField,
                        JExpr._new(codeModel.ref(HashMap.class).narrow(Integer.class).narrow(Schema.class)));
                registerSchema(aliasedWriterSchema, readerSchemaVar);
            }

            JClass readerSchemaClass = schemaAnalyzer.classFromSchema(reader);
            JClass writerSchemaClass = schemaAnalyzer.classFromSchema(aliasedWriterSchema);

            deserializerClass._implements(codeModel.ref(FastDeserializer.class).narrow(writerSchemaClass));
            JMethod deserializeMethod = deserializerClass.method(JMod.PUBLIC, readerSchemaClass, "deserialize");

            JVar result = declareContainerVariableForSchemaInBlock("result", aliasedWriterSchema,
                    deserializeMethod.body());

            JTryBlock tryDeserializeBlock = deserializeMethod.body()._try();

            switch (aliasedWriterSchema.getType()) {
            case RECORD:
                processRecord(readerSchemaVar, result, aliasedWriterSchema, reader, tryDeserializeBlock.body(),
                        fieldAction);
                break;
            case ARRAY:
                processArray(readerSchemaVar, result, null, aliasedWriterSchema, reader, tryDeserializeBlock.body(),
                        fieldAction);
                break;
            case MAP:
                processMap(readerSchemaVar, result, null, aliasedWriterSchema, reader, tryDeserializeBlock.body(),
                        fieldAction);
                break;
            default:
                throw new FastDeserializerGeneratorException(
                        "Incorrect top-level writer schema: " + aliasedWriterSchema.getType());
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

    private void processRecord(JVar recordSchemaVar, JVar recordVar,
            final Schema recordWriterSchema, final Schema recordReaderSchema,
            JBlock body, FieldAction recordAction) {

        ListIterator<Symbol> actionIterator = actionIterator(recordAction);

        if (methodAlreadyDefined(recordWriterSchema, recordAction.getShouldRead())) {
            JExpression readingExpression = JExpr.invoke(getMethod(recordWriterSchema, recordAction.getShouldRead()))
                    .arg(JExpr.direct(DECODER));
            if (recordVar != null) {
                body.assign(recordVar, readingExpression);
            } else {
                body.add((JStatement) readingExpression);
            }

            // seek through actionIterator
            for (Schema.Field field : recordWriterSchema.getFields()) {
                FieldAction action = seekFieldAction(recordAction.getShouldRead(), field, actionIterator);
                if (action.getSymbol() == END_SYMBOL) {
                    break;
                }
            }
            if (!recordAction.getShouldRead()) {
                return;
            }
            // seek through actionIterator also for default values
            Set<String> fieldNamesSet = recordWriterSchema.getFields().stream().map(Schema.Field::name)
                    .collect(Collectors.toSet());
            for (Schema.Field readerField : recordReaderSchema.getFields()) {
                if (!fieldNamesSet.contains(readerField.name())) {
                    forwardToExpectedDefault(actionIterator);
                    seekFieldAction(true, readerField, actionIterator);
                }
            }
            return;
        } else {
            JMethod method = createMethod(recordWriterSchema, recordAction.getShouldRead());
            method._throws(Throwable.class);

            if (recordVar != null) {
                body.assign(recordVar,
                        JExpr.invoke(getMethod(recordWriterSchema, recordAction.getShouldRead()))
                                .arg(JExpr.direct(DECODER)));
            } else {
                body.invoke(getMethod(recordWriterSchema, recordAction.getShouldRead())).arg(JExpr.direct(DECODER));
            }

            body = method.body();
        }

        JVar result = null;
        if (recordAction.getShouldRead()) {
            JClass recordClass = schemaAnalyzer.classFromSchema(recordWriterSchema);
            JInvocation newRecord = JExpr._new(schemaAnalyzer.classFromSchema(recordWriterSchema, false));
            if (useGenericTypes) {
                newRecord = newRecord.arg(schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(recordWriterSchema))));
            }
            result = body.decl(recordClass, "result", newRecord);
        }

        for (Schema.Field field : recordWriterSchema.getFields()) {

            FieldAction action = seekFieldAction(recordAction.getShouldRead(), field, actionIterator);
            if (action.getSymbol() == END_SYMBOL) {
                break;
            }

            Schema.Field readerField = null;
            Schema readerFieldSchema = null;
            JVar fieldValueVar = null;
            JVar fieldSchemaVar = null;
            if (action.getShouldRead()) {
                fieldValueVar = body.decl(schemaAnalyzer.classFromSchema(field.schema()), getVariableName(field.name()),
                        JExpr._null());
                readerField = recordReaderSchema.getField(field.name());
                readerFieldSchema = readerField.schema();
                if (useGenericTypes)
                    fieldSchemaVar = declareSchemaVar(field.schema(), field.name(),
                            recordSchemaVar.invoke("getField").arg(field.name()).invoke("schema"));
            }

            JExpression fieldValueExpression;
            if (SchemaAnalyzer.isComplexType(field.schema())) {
                switch (field.schema().getType()) {
                case RECORD:
                    processRecord(fieldSchemaVar, fieldValueVar, field.schema(), readerFieldSchema, body, action);
                    break;
                case ARRAY:
                    processArray(fieldSchemaVar, fieldValueVar, field.name(), field.schema(), readerFieldSchema, body,
                            action);
                    break;
                case MAP:
                    processMap(fieldSchemaVar, fieldValueVar, field.name(), field.schema(), readerFieldSchema, body,
                            action);
                    break;
                case UNION:
                    processUnion(fieldSchemaVar, fieldValueVar, field.name(), field.schema(), readerFieldSchema, body,
                            action);
                    break;
                default:
                    throw new FastDeserializerGeneratorException("Incorrect complex type: " + action.getType());
                }
                fieldValueExpression = fieldValueVar;
            } else {
                switch (action.getType()) {
                case ENUM:
                    fieldValueExpression = processEnum(field.schema(), body, action);
                    break;
                case FIXED:
                    fieldValueExpression = processFixed(field.schema(), body, action);
                    break;
                default:
                    fieldValueExpression = processPrimitive(field.schema(), body, action);
                }

            }
            if (action.getShouldRead()) {
                if (fieldValueExpression == null) {
                    throw new FastDeserializerGeneratorException(
                            "Value expression was expected, got null instead!");
                }
                body.invoke(result, "put").arg(JExpr.lit(readerField.pos())).arg(fieldValueExpression);
            }
        }

        // Handle default values
        if (recordAction.getShouldRead()) {
            Set<String> fieldNamesSet = recordWriterSchema.getFields().stream().map(Schema.Field::name)
                    .collect(Collectors.toSet());
            for (Schema.Field readerField : recordReaderSchema.getFields()) {
                if (!fieldNamesSet.contains(readerField.name())) {
                    forwardToExpectedDefault(actionIterator);
                    seekFieldAction(true, readerField, actionIterator);
                    JVar schemaVar = null;
                    if (useGenericTypes) {
                        schemaVar = declareSchemaVariableForRecordField(readerField.name(), readerField.schema(),
                                recordSchemaVar);
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
        if (Schema.Type.UNION.equals(schemaType)) {
            schema = schema.getTypes().get(0);
            schemaType = schema.getType();
            schemaVariable = declareSchemaVariableForUnionOption(fieldName, schema, schemaVariable, 0);
        }
        if (Schema.Type.NULL.equals(schemaType)) {
            return JExpr._null();
        }

        JClass defaultValueClass = schemaAnalyzer.classFromSchema(schema, false);
        //TODO: New schema in defaults
        switch (schemaType) {
        case RECORD:
            JInvocation valueInitializationExpr = JExpr._new(defaultValueClass);
            if (useGenericTypes) {
                valueInitializationExpr = valueInitializationExpr
                        .arg(schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema))));
            }
            JVar recordVar = body.decl(defaultValueClass, getVariableName("default" + schema.getName()),
                    valueInitializationExpr);
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

        case ARRAY:
            Schema elementSchema = schema.getElementType();
            JVar elementSchemaVariable = null;
            JVar arrayVar;
            JInvocation arrayVarInitialization = JExpr._new(defaultValueClass);

            if (useGenericTypes) {
                JInvocation getSchema = schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema)));
                int elementCount = defaultValue.size();
                arrayVarInitialization = arrayVarInitialization.arg(JExpr.lit(elementCount)).arg(getSchema);
                elementSchemaVariable = declareSchemaVar(elementSchema, "defaultElementSchema",
                        schemaVariable.invoke("getElementType"));
            }

            arrayVar = body.decl(defaultValueClass, getVariableName("defaultArray"), arrayVarInitialization);

            for (JsonNode arrayEntryValue : defaultValue) {
                JExpression fieldValue = parseDefaultValue(elementSchema, arrayEntryValue, body, elementSchemaVariable,
                        "arrayValue");
                body.invoke(arrayVar, "add").arg(fieldValue);
            }
            return arrayVar;
        case MAP:
            JVar mapVar = body.decl(defaultValueClass, getVariableName("defaultMap"),
                    JExpr._new(schemaAnalyzer.classFromSchema(schema, false)));

            JVar mapValueSchemaVar = null;
            if (useGenericTypes) {
                mapValueSchemaVar = declareSchemaVar(schema.getValueType(), "defaultValueSchema",
                        schemaVariable.invoke("getValueType"));
            }
            for (Iterator<Map.Entry<String, JsonNode>> it = defaultValue.getFields(); it.hasNext();) {
                Map.Entry<String, JsonNode> mapEntry = it.next();
                JExpression fieldValue = parseDefaultValue(schema.getValueType(), mapEntry.getValue(), body,
                        mapValueSchemaVar, "mapElement");
                body.invoke(mapVar, "put").arg(mapEntry.getKey()).arg(fieldValue);
            }
            return mapVar;
        case ENUM:
            String enumLabel = defaultValue.getTextValue();
            if (!useGenericTypes) {
                return codeModel.ref(schema.getFullName()).staticInvoke("valueOf").arg(enumLabel);
            } else {
                JInvocation getSchema = schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema)));
                return JExpr._new(codeModel.ref(GenericData.EnumSymbol.class)).arg(getSchema).arg(enumLabel);
            }
        case FIXED:
            JArray fixedBytesArray = JExpr.newArray(codeModel.BYTE);
            for (char b : defaultValue.getTextValue().toCharArray()) {
                fixedBytesArray.add(JExpr.lit((byte) b));
            }
            if (!useGenericTypes) {
                return JExpr._new(codeModel.ref(schema.getFullName())).arg(fixedBytesArray);
            } else {
                JInvocation getSchema = schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema)));
                return JExpr._new(codeModel.ref(GenericData.Fixed.class)).arg(getSchema).arg(fixedBytesArray);
            }
        case BYTES:
            JArray bytesArray = JExpr.newArray(codeModel.BYTE);
            for (byte b : defaultValue.getTextValue().getBytes()) {
                bytesArray.add(JExpr.lit(b));
            }
            return codeModel.ref(ByteBuffer.class).staticInvoke("wrap").arg(bytesArray);
        case STRING:
            return JExpr.lit(defaultValue.getTextValue());
        case INT:
            return JExpr.lit(defaultValue.getIntValue());
        case LONG:
            return JExpr.lit(defaultValue.getLongValue());
        case FLOAT:
            return JExpr.lit((float) defaultValue.getDoubleValue());
        case DOUBLE:
            return JExpr.lit(defaultValue.getDoubleValue());
        case BOOLEAN:
            return JExpr.lit(defaultValue.getBooleanValue());
        case NULL:
        case UNION:
        default:
            throw new FastDeserializerGeneratorException("Incorrect schema type in default value!");
        }
    }

    private void processUnion(JVar unionSchemaVar, JVar unionValueVar, final String name,
            final Schema unionSchema, final Schema readerUnionSchema,
            JBlock body, FieldAction action) {
        JVar unionIndex = body.decl(codeModel.INT, getVariableName("unionIndex"),
                JExpr.direct(DECODER + ".readIndex()"));
        JClass unionClass = schemaAnalyzer.classFromSchema(unionSchema);

        for (int i = 0; i < unionSchema.getTypes().size(); i++) {
            Schema optionSchema = unionSchema.getTypes().get(i);
            Schema readerOptionSchema = null;
            FieldAction unionAction;

            if (Schema.Type.NULL.equals(optionSchema.getType())) {
                body._if(unionIndex.eq(JExpr.lit(i)))._then().directStatement(DECODER + ".readNull();");
                continue;
            }

            if (action.getShouldRead()) {
                readerOptionSchema = readerUnionSchema.getTypes().get(i);
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
                unionAction = FieldAction.fromValues(optionSchema.getType(), action.getShouldRead(),
                        unionAdjustAction.symToParse);
            } else {
                unionAction = FieldAction.fromValues(optionSchema.getType(), false, EMPTY_SYMBOL);
            }

            JBlock ifBlock = body._if(unionIndex.eq(JExpr.lit(i)))._then();

            JVar optionSchemaVar = null;
            if (unionAction.getShouldRead()) {
                optionSchemaVar = declareSchemaVariableForUnionOption(name, optionSchema, unionSchemaVar, i);
            }

            JExpression optionValue;
            if (SchemaAnalyzer.isComplexType(optionSchema)) {

                JVar optionValueVar = null;

                if (unionAction.getShouldRead()) {
                    optionValueVar = declareContainerVariableForSchemaInBlock(name, optionSchema, ifBlock);
                }
                switch (unionAction.getType()) {
                case RECORD:
                    processRecord(optionSchemaVar, optionValueVar, optionSchema, readerOptionSchema, ifBlock,
                            unionAction);
                    break;
                case ARRAY:
                    processArray(optionSchemaVar, optionValueVar, name, optionSchema, readerOptionSchema, ifBlock,
                            unionAction);
                    break;
                case MAP:
                    processMap(optionSchemaVar, optionValueVar, name, optionSchema, readerOptionSchema, ifBlock,
                            unionAction);
                    break;
                default:
                    throw new FastDeserializerGeneratorException("Incorrect complex type: " + action.getType());
                }
                optionValue = optionValueVar;
            } else {
                if (Schema.Type.ENUM.equals(unionAction.getType())) {
                    optionValue = processEnum(optionSchema, ifBlock, unionAction);
                } else if (Schema.Type.FIXED.equals(unionAction.getType())) {
                    optionValue = processFixed(optionSchema, ifBlock, unionAction);
                } else {
                    optionValue = processPrimitive(optionSchema, ifBlock, unionAction);
                }
            }
            if (unionAction.getShouldRead()) {
                if (optionValue == null) {
                    throw new FastDeserializerGeneratorException(
                            "Value expression was expected, got null instead!");
                }
                ifBlock.assign(unionValueVar, JExpr.cast(unionClass, optionValue));
            }
        }
    }

    private void processArray(JVar arraySchemaVar, JVar arrayValueVar, final String name,
            final Schema arraySchema, final Schema readerArraySchema,
            JBlock body, FieldAction action) {

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

        JVar chunkLen = body.decl(codeModel.LONG, getVariableName("chunkLen"),
                JExpr.direct(DECODER + ".readArrayStart()"));

        JConditional conditional = body._if(chunkLen.gt(JExpr.lit(0)));
        JBlock ifBlock = conditional._then();

        JClass arrayClass = schemaAnalyzer.classFromSchema(arraySchema, false);

        if (action.getShouldRead()) {
            if (useGenericTypes) {
                ifBlock.assign(arrayValueVar, JExpr._new(arrayClass).arg(JExpr.cast(codeModel.INT, chunkLen))
                        .arg(schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(arraySchema)))));
            } else {
                ifBlock.assign(arrayValueVar, JExpr._new(arrayClass));
            }
            JBlock elseBlock = conditional._else();
            if (useGenericTypes) {
                elseBlock.assign(arrayValueVar, JExpr._new(arrayClass).arg(JExpr.lit(0))
                        .arg(schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(arraySchema)))));
            } else {
                elseBlock.assign(arrayValueVar, codeModel.ref(Collections.class).staticInvoke("emptyList"));
            }
        }

        JDoLoop doLoop = ifBlock._do(chunkLen.gt(JExpr.lit(0)));
        JForLoop forLoop = doLoop.body()._for();
        JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
        forLoop.test(counter.lt(chunkLen));
        forLoop.update(counter.incr());
        JBlock forBody = forLoop.body();

        Schema readerArrayElementSchema = null;
        JExpression elementValueExpression;

        if (SchemaAnalyzer.isComplexType(arraySchema.getElementType())) {
            JVar elementSchemaVar = null;
            JVar elementValueVar = null;
            if (action.getShouldRead()) {
                elementValueVar = declareContainerVariableForSchemaInBlock(name, arraySchema.getElementType(), forBody);
                readerArrayElementSchema = readerArraySchema.getElementType();
                if (useGenericTypes) {
                    elementSchemaVar = schemaMapMethod.body().decl(codeModel.ref(Schema.class),
                            getVariableName(name + "ArraySchema"), arraySchemaVar.invoke("getElementType"));
                    registerSchema(arraySchema.getElementType(), elementSchemaVar);
                }
            }
            String elemName = name + "Elem";

            switch (arraySchema.getElementType().getType()) {
            case RECORD:
                processRecord(elementSchemaVar, elementValueVar, arraySchema.getElementType(), readerArrayElementSchema,
                        forBody, action);
                break;
            case ARRAY:
                processArray(elementSchemaVar, elementValueVar, elemName, arraySchema.getElementType(),
                        readerArrayElementSchema, forBody, action);
                break;
            case MAP:
                processMap(elementSchemaVar, elementValueVar, elemName, arraySchema.getElementType(),
                        readerArrayElementSchema,
                        forBody, action);
                break;
            case UNION:
                processUnion(elementSchemaVar, elementValueVar, elemName, arraySchema.getElementType(),
                        readerArrayElementSchema, forBody, action);
                break;
            }
            elementValueExpression = elementValueVar;
        } else {
            switch (arraySchema.getElementType().getType()) {
            case ENUM:
                elementValueExpression = processEnum(arraySchema.getElementType(), forBody, action);
                break;
            case FIXED:
                elementValueExpression = processFixed(arraySchema.getElementType(), forBody, action);
                break;
            default:
                elementValueExpression = processPrimitive(arraySchema.getElementType(), forBody, action);
            }
        }
        if (action.getShouldRead()) {
            if (elementValueExpression == null) {
                throw new FastDeserializerGeneratorException(
                        "Value expression was expected, got null instead!");
            }
            forBody.invoke(arrayValueVar, "add").arg(elementValueExpression);
        }
        doLoop.body().assign(chunkLen, JExpr.direct(DECODER + ".arrayNext()"));
    }

    private void processMap(JVar mapSchemaVar, JVar containerVariable, final String name, final Schema mapSchema,
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

        JVar chunkLen = body.decl(codeModel.LONG, getVariableName("chunkLen"),
                JExpr.direct(DECODER + ".readMapStart()"));

        JConditional conditional = body._if(chunkLen.gt(JExpr.lit(0)));
        JBlock ifBlock = conditional._then();

        if (action.getShouldRead()) {
            ifBlock.assign(containerVariable, JExpr._new(schemaAnalyzer.classFromSchema(mapSchema, false)));
            JBlock elseBlock = conditional._else();
            elseBlock.assign(containerVariable, codeModel.ref(Collections.class).staticInvoke("emptyMap"));
        }

        JDoLoop doLoop = ifBlock._do(chunkLen.gt(JExpr.lit(0)));
        JForLoop forLoop = doLoop.body()._for();
        JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
        forLoop.test(counter.lt(chunkLen));
        forLoop.update(counter.incr());
        JBlock forBody = forLoop.body();

        JClass keyClass = schemaAnalyzer.keyClassFromMapSchema(mapSchema);
        JExpression keyValueExpression = JExpr.direct(DECODER + ".readString()");
        if (!keyClass.name().equals(String.class.getName())) {
            keyValueExpression = JExpr._new(keyClass).arg(keyValueExpression);
        }

        JVar key = forBody.decl(keyClass, getVariableName("key"), keyValueExpression);
        JExpression mapValueExpression = null;
        JVar mapValueSchemaVar = null;
        if (useGenericTypes)
            mapValueSchemaVar = declareSchemaVar(mapSchema.getValueType(), getVariableName(name + "MapValue"),
                    mapSchemaVar.invoke("getValueType"));

        if (SchemaAnalyzer.isComplexType(mapSchema.getValueType())) {
            JVar mapValueVar = null;
            Schema readerMapValueSchema = readerMapSchema.getValueType();

            if (action.getShouldRead()) {
                mapValueVar = declareContainerVariableForSchemaInBlock(name, mapSchema.getValueType(), forBody);
            }
            switch (mapSchema.getValueType().getType()) {
            case RECORD:
                processRecord(mapValueSchemaVar, mapValueVar, mapSchema.getValueType(), readerMapValueSchema, forBody,
                        action);
                break;
            case ARRAY:
                processArray(mapValueSchemaVar, mapValueVar, name, mapSchema.getValueType(), readerMapValueSchema,
                        forBody, action);
                break;
            case MAP:
                processMap(mapValueSchemaVar, mapValueVar, name, mapSchema.getValueType(), readerMapValueSchema,
                        forBody,
                        action);
                break;
            case UNION:
                processUnion(mapValueSchemaVar, mapValueVar, name, mapSchema.getValueType(), readerMapValueSchema,
                        forBody, action);
                break;
            default:
                throw new FastDeserializerGeneratorException("Incorrect complex type: " + action.getType());
            }
            mapValueExpression = mapValueVar;
        } else {
            switch (mapSchema.getValueType().getType()) {
            case ENUM:
                mapValueExpression = processEnum(mapSchema.getValueType(), forBody, action);
                break;
            case FIXED:
                mapValueExpression = processFixed(mapSchema.getValueType(), forBody, action);
                break;
            default:
                mapValueExpression = processPrimitive(mapSchema.getValueType(), forBody, action);
            }
        }
        doLoop.body().assign(chunkLen, JExpr.direct(DECODER + ".mapNext()"));

        if (action.getShouldRead()) {
            if (mapValueExpression == null) {
                throw new FastDeserializerGeneratorException(
                        "Value expression was expected, got null instead!");
            }
            forBody.invoke(containerVariable, "put").arg(key).arg(mapValueExpression);
        }
    }

    private JExpression processFixed(final Schema schema, JBlock body, FieldAction action) {
        if (action.getShouldRead()) {
            JInvocation getSchema = null;
            if (useGenericTypes) {
                getSchema = schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema)));
            }

            JVar fixedBuffer = body.decl(codeModel.ref(byte[].class), getVariableName(schema.getName()))
                    .init(JExpr.direct(" new byte[" + schema.getFixedSize() + "]"));

            body.directStatement(DECODER + ".readFixed(" + fixedBuffer.name() + ");");
            JExpression fixed = useGenericTypes
                    ? JExpr._new(codeModel.ref(GenericData.Fixed.class)).arg(getSchema).arg(fixedBuffer)
                    : JExpr._new(codeModel.ref(schema.getFullName())).arg(fixedBuffer);

            return fixed;
        } else {
            body.directStatement(DECODER + ".skipFixed(" + schema.getFixedSize() + ");");
            return null;
        }
    }

    private JExpression processEnum(final Schema schema, final JBlock body, FieldAction action) {

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
                    throw new FastDeserializerGeneratorException(
                            schema.getName() + " enum label impossible to deserialize: " + adjustment.toString());
                } else if (!adjustment.equals(i)) {
                    enumOrderCorrect = false;
                }
            }

            JExpression newEnum;
            if (enumOrderCorrect) {
                newEnum = useGenericTypes
                        ? JExpr._new(codeModel.ref(GenericData.EnumSymbol.class)).arg(getSchema)
                                .arg(getSchema.invoke("getEnumSymbols").invoke("get")
                                        .arg(JExpr.direct(DECODER + ".readEnum()")))
                        : codeModel.ref(schema.getFullName()).staticInvoke("values")
                                .component(JExpr.direct(DECODER + ".readEnum()"));
            } else {
                JVar enumIndex = body.decl(codeModel.INT, getVariableName("enumIndex"),
                        JExpr.direct(DECODER + ".readEnum()"));
                newEnum = useGenericTypes
                        ? body.decl(codeModel.ref(GenericData.EnumSymbol.class), getVariableName("enumValue"),
                                JExpr._null())
                        : body.decl(codeModel.ref(schema.getFullName()), getVariableName("enumValue"), JExpr._null());

                for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
                    if (useGenericTypes) {
                        body._if(enumIndex.eq(JExpr.lit(i)))._then().assign((JVar) newEnum,
                                JExpr._new(codeModel.ref(GenericData.EnumSymbol.class)).arg(getSchema)
                                        .arg(getSchema.invoke("getEnumSymbols").invoke("get")
                                                .arg(JExpr.lit((Integer) enumAdjustAction.adjustments[i]))));
                    } else {
                        body._if(enumIndex.eq(JExpr.lit(i)))._then().assign((JVar) newEnum,
                                codeModel.ref(schema.getFullName()).staticInvoke("values")
                                        .component(JExpr.lit((Integer) enumAdjustAction.adjustments[i])));
                    }
                }
            }

            return newEnum;
        } else {
            body.directStatement(DECODER + ".readEnum();");
            return null;
        }

    }

    private JExpression processPrimitive(final Schema fieldSchema, JBlock body, FieldAction action) {

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
                    primitiveValueExpression = JExpr._new(codeModel.ref(Class.forName(valueJavaClassName)))
                            .arg(primitiveValueExpression);
                } catch (ClassNotFoundException e) {
                    throw new FastDeserializerGeneratorException("Unknown value java class: " + valueJavaClassName);
                }
            }

            return primitiveValueExpression;
        } else {
            body.directStatement(DECODER + "." + readFunction + ";");
            return JExpr._null();
        }
    }

    private JVar declareContainerVariableForSchemaInBlock(final String name, final Schema schema, JBlock block) {
        if (SchemaAnalyzer.isComplexType(schema)) {
            return block.decl(schemaAnalyzer.classFromSchema(schema), getVariableName(name), JExpr._null());
        }
        return null;
    }

    private JVar declareSchemaVar(Schema valueSchema, String variableName, JInvocation getValueType) {
        if (!useGenericTypes) {
            return null;
        }
        if (SchemaAnalyzer.isComplexType(valueSchema) || Schema.Type.ENUM.equals(valueSchema.getType())) {
            JVar schemaVar = schemaMapMethod.body().decl(codeModel.ref(Schema.class), getVariableName(variableName),
                    getValueType);
            registerSchema(valueSchema, schemaVar);
            return schemaVar;
        } else {
            return null;
        }
    }

    private JVar declareSchemaVariableForUnionOption(final String name, final Schema unionOptionSchema, JVar schemaVar,
            int unionOptionNumber) {
        if (!useGenericTypes) {
            return null;
        }
        return declareSchemaVar(unionOptionSchema, name,
                schemaVar.invoke("getTypes").invoke("get").arg(JExpr.lit(unionOptionNumber)));
    }

    private JVar declareSchemaVariableForRecordField(final String name, final Schema schema, JVar schemaVar) {
        if (!useGenericTypes) {
            return null;
        }

        JVar fieldSchemaVar = schemaMapMethod.body().decl(codeModel.ref(Schema.class), getVariableName(name + "Schema"),
                schemaVar.invoke("getField").arg(name).invoke("schema"));
        registerSchema(schema, fieldSchemaVar);
        return fieldSchemaVar;
    }

    private boolean methodAlreadyDefined(final Schema schema, boolean read) {
        if (read) {
            return !Schema.Type.RECORD.equals(schema.getType())
                    || deserializeMethodMap.containsKey(schema.getFullName());
        }
        return !Schema.Type.RECORD.equals(schema.getType()) || skipMethodMap.containsKey(schema.getFullName());
    }

    private JMethod getMethod(final Schema schema, boolean read) {
        if (Schema.Type.RECORD.equals(schema.getType())) {
            if (methodAlreadyDefined(schema, read)) {
                return read ? deserializeMethodMap.get(schema.getFullName()) : skipMethodMap.get(schema.getFullName());
            }
            throw new FastDeserializerGeneratorException("No method for schema: " + schema.getFullName());
        }
        throw new FastDeserializerGeneratorException("No method for schema type: " + schema.getType());
    }

    private JMethod createMethod(final Schema schema, boolean read) {
        if (Schema.Type.RECORD.equals(schema.getType())) {
            if (!methodAlreadyDefined(schema, read)) {
                JMethod method;
                if (useGenericTypes) {
                    method = deserializerClass.method(JMod.PUBLIC,
                            read ? codeModel.ref(GenericData.Record.class) : codeModel.VOID,
                            "deserialize" + schema.getName() + nextRandomInt());
                } else {
                    method = deserializerClass.method(JMod.PUBLIC,
                            read ? codeModel.ref(schema.getFullName()) : codeModel.VOID,
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

    private void registerSchema(final Schema writerSchema, JVar schemaVar) {
        if ((Schema.Type.RECORD.equals(writerSchema.getType()) || Schema.Type.ENUM.equals(writerSchema.getType())
                || Schema.Type.ARRAY.equals(writerSchema.getType())) && doesNotContainSchema(writerSchema)) {
            schemaMap.put(getSchemaId(writerSchema), writerSchema);

            schemaMapMethod.body().invoke(schemaMapField, "put").arg(JExpr.lit(getSchemaId(writerSchema)))
                    .arg(schemaVar);
        }
    }

    private boolean doesNotContainSchema(final Schema schema) {
        return !schemaMap.containsKey(getSchemaId(schema));
    }

}
