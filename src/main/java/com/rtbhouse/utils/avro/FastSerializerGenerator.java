package com.rtbhouse.utils.avro;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Encoder;

import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JForEach;
import com.sun.codemodel.JForLoop;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPackage;
import com.sun.codemodel.JVar;

public class FastSerializerGenerator<T> extends FastSerializerGeneratorBase<T> {

    private static final String ENCODER = "encoder";

    private boolean useGenericTypes;
    private Map<String, JMethod> serializeMethodMap = new HashMap<>();
    private SchemaAnalyzer schemaAnalyzer;

    public FastSerializerGenerator(boolean useGenericTypes, Schema schema, File destination, ClassLoader classLoader,
            String compileClassPath) {
        super(schema, destination, classLoader, compileClassPath);
        this.useGenericTypes = useGenericTypes;
        this.schemaAnalyzer = new SchemaAnalyzer(codeModel, useGenericTypes);
    }

    @Override
    public FastSerializer<T> generateSerializer() {
        final String className = getClassName(schema, useGenericTypes ? "Generic" : "Specific");
        final JPackage classPackage = codeModel._package(GENERATED_PACKAGE_NAME);

        try {
            serializerClass = classPackage._class(className);

            final JMethod serializeMethod = serializerClass.method(JMod.PUBLIC, void.class, "serialize");
            final JVar serializeMethodParam;

            JClass outputClass = schemaAnalyzer.classFromSchema(schema);
            serializerClass._implements(codeModel.ref(FastSerializer.class).narrow(outputClass));
            serializeMethodParam = serializeMethod.param(outputClass, "data");

            switch (schema.getType()) {
            case RECORD:
                processRecord(schema, serializeMethodParam, serializeMethod.body());
                break;
            case ARRAY:
                processArray(schema, serializeMethodParam, serializeMethod.body());
                break;
            case MAP:
                processMap(schema, serializeMethodParam, serializeMethod.body());
                break;
            default:
                throw new FastSerializerGeneratorException("Unsupported input schema type: " + schema.getType());
            }

            serializeMethod.annotate(SuppressWarnings.class).param("value", "unchecked");
            serializeMethod.param(codeModel.ref(Encoder.class), ENCODER);
            serializeMethod._throws(codeModel.ref(IOException.class));

            final Class<FastSerializer<T>> clazz = compileClass(className);
            return clazz.newInstance();
        } catch (JClassAlreadyExistsException e) {
            throw new FastSerializerGeneratorException("Class: " + className + " already exists");
        } catch (Exception e) {
            throw new FastSerializerGeneratorException(e);
        }
    }

    private void processRecord(final Schema recordSchema, JVar recordVariable, final JBlock containerBody) {
        if (!doesNotContainMethod(recordSchema)) {
            containerBody.invoke(getMethod(recordSchema)).arg(recordVariable).arg(JExpr.direct(ENCODER));
            return;
        }
        JMethod method = createMethod(recordSchema);
        containerBody.invoke(getMethod(recordSchema)).arg(recordVariable).arg(JExpr.direct(ENCODER));

        JBlock body = method.body();
        recordVariable = method.listParams()[0];

        for (Schema.Field field : recordSchema.getFields()) {

            Schema fieldSchema = field.schema();
            JVar containerVar = null;
            if (SchemaAnalyzer.isContainerType(fieldSchema)) {
                JClass fieldClass = schemaAnalyzer.classFromSchema(fieldSchema);
                containerVar = declareContainerVariableForSchemaInBlock(getVariableName(field.name()), fieldSchema,
                        body);
                JExpression valueExpression = JExpr.invoke(recordVariable, "get").arg(JExpr.lit(field.pos()));
                containerVar.init(JExpr.cast(fieldClass, valueExpression));
            }

            switch (fieldSchema.getType()) {
            case RECORD:
                processRecord(fieldSchema, containerVar, body);
                break;
            case ARRAY:
                processArray(fieldSchema, containerVar, body);
                break;
            case MAP:
                processMap(fieldSchema, containerVar, body);
                break;
            case ENUM:
                processEnum(recordVariable, recordSchema, field, body);
                break;
            case UNION:
                processUnion(recordVariable, recordSchema, fieldSchema, field, body);
                break;
            case FIXED:
                processFixed(recordVariable, recordSchema, field, body);
                break;
            default:
                processPrimitive(recordVariable, recordSchema, fieldSchema, field, body);
            }

        }
    }

    private void processArray(final Schema arraySchema, JVar arrayVariable, JBlock body) {
        final JClass arrayClass = codeModel.ref(List.class)
                .narrow(schemaAnalyzer.elementClassFromArraySchema(arraySchema));
        body.invoke(JExpr.direct(ENCODER), "writeArrayStart");

        final JExpression emptyArrayCondition = arrayVariable.eq(JExpr._null())
                .cor(JExpr.invoke(JExpr.cast(arrayClass, arrayVariable), "size").eq(JExpr.lit(0)));

        final JConditional emptyArrayIf = body._if(emptyArrayCondition);
        final JBlock emptyArrayBlock = emptyArrayIf._then();
        emptyArrayBlock.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.lit(0));
        emptyArrayBlock.invoke(JExpr.direct(ENCODER), "writeArrayEnd");

        final JBlock nonEmptyArrayBlock = emptyArrayIf._else();
        nonEmptyArrayBlock.invoke(JExpr.direct(ENCODER), "setItemCount")
                .arg(JExpr.invoke(JExpr.cast(arrayClass, arrayVariable), "size"));
        final JForLoop forLoop = nonEmptyArrayBlock._for();
        final JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
        forLoop.test(counter.lt(JExpr.invoke(JExpr.cast(arrayClass, arrayVariable), "size")));
        forLoop.update(counter.incr());
        final JBlock forBody = forLoop.body();
        forBody.invoke(JExpr.direct(ENCODER), "startItem");

        final Schema elementSchema = arraySchema.getElementType();
        JVar containerVar = null;
        if (SchemaAnalyzer.isContainerType(elementSchema)) {
            containerVar = declareContainerVariableForSchemaInBlock(getVariableName(elementSchema.getName()),
                    elementSchema, forBody);
            forBody.assign(containerVar, JExpr.invoke(JExpr.cast(arrayClass, arrayVariable), "get").arg(counter));
        }

        switch (elementSchema.getType()) {

        case RECORD:
            processRecord(elementSchema, containerVar, forBody);
            break;
        case ENUM:
            processEnum(arrayVariable, arraySchema, counter, forBody);
            break;
        case ARRAY:
            processArray(elementSchema, containerVar, forBody);
            break;
        case MAP:
            processMap(elementSchema, containerVar, forBody);
            break;
        case UNION:
            processUnion(arrayVariable, arraySchema, elementSchema, counter, forBody);
            break;
        case FIXED:
            processFixed(arrayVariable, arraySchema, counter, forBody);
            break;
        default:
            processPrimitive(arrayVariable, arraySchema, elementSchema, counter, forBody);
        }

        nonEmptyArrayBlock.invoke(JExpr.direct(ENCODER), "writeArrayEnd");

    }

    private void processMap(final Schema mapSchema, JVar containerVariable, JBlock body) {

        final JClass mapType = schemaAnalyzer.classFromSchema(mapSchema);

        JClass keyClass = schemaAnalyzer.keyClassFromMapSchema(mapSchema);

        body.invoke(JExpr.direct(ENCODER), "writeMapStart");

        final JExpression emptyMapCondition = containerVariable.eq(JExpr._null())
                .cor(JExpr.invoke(JExpr.cast(mapType, containerVariable), "size").eq(JExpr.lit(0)));
        final JConditional emptyMapIf = body._if(emptyMapCondition);
        final JBlock emptyMapBlock = emptyMapIf._then();
        emptyMapBlock.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.lit(0));
        emptyMapBlock.invoke(JExpr.direct(ENCODER), "writeMapEnd");

        final JBlock nonEmptyMapBlock = emptyMapIf._else();
        nonEmptyMapBlock.invoke(JExpr.direct(ENCODER), "setItemCount")
                .arg(JExpr.invoke(JExpr.cast(mapType, containerVariable), "size"));

        final JForEach mapKeysLoop = nonEmptyMapBlock.forEach(keyClass, getVariableName("key"),
                JExpr.invoke(JExpr.cast(mapType, containerVariable), "keySet"));

        final JBlock forBody = mapKeysLoop.body();
        forBody.invoke(JExpr.direct(ENCODER), "startItem");

        JVar keyStringVar;
        if (!String.class.getName().equals(keyClass.name())) {
            keyStringVar = forBody.decl(codeModel.ref(String.class), getVariableName("keyString"),
                    mapKeysLoop.var().invoke("toString"));
        } else {
            keyStringVar = mapKeysLoop.var();
        }

        final Schema valueSchema = mapSchema.getValueType();

        forBody.invoke(JExpr.direct(ENCODER), "writeString").arg(keyStringVar);
        if (Schema.Type.RECORD.equals(valueSchema.getType())) {
            JVar containerVar = declareContainerVariableForSchemaInBlock(valueSchema.getName(), valueSchema,
                    forBody);
            forBody.assign(containerVar,
                    JExpr.invoke(JExpr.cast(mapType, containerVariable), "get").arg(mapKeysLoop.var()));
            processRecord(valueSchema, containerVar, forBody);
        } else if (Schema.Type.ARRAY.equals(valueSchema.getType())) {
            JVar containerVar = declareContainerVariableForSchemaInBlock(valueSchema.getName(), valueSchema, forBody);
            forBody.assign(containerVar,
                    JExpr.invoke(JExpr.cast(mapType, containerVariable), "get").arg(mapKeysLoop.var()));
            processArray(valueSchema, containerVar, forBody);
        } else if (Schema.Type.MAP.equals(valueSchema.getType())) {
            JVar containerVar = declareContainerVariableForSchemaInBlock(valueSchema.getName(), valueSchema, forBody);
            forBody.assign(containerVar,
                    JExpr.invoke(JExpr.cast(mapType, containerVariable), "get").arg(mapKeysLoop.var()));
            processMap(valueSchema, containerVar, forBody);
        } else if (Schema.Type.ENUM.equals(valueSchema.getType())) {
            processEnum(containerVariable, mapKeysLoop.var(), mapSchema, forBody);
        } else if (Schema.Type.FIXED.equals(valueSchema.getType())) {
            processFixed(containerVariable, mapKeysLoop.var(), mapSchema, forBody);
        } else if (Schema.Type.UNION.equals(valueSchema.getType())) {
            processUnion(containerVariable, mapKeysLoop.var(), mapSchema, valueSchema, forBody);
        } else {
            processPrimitive(containerVariable, mapKeysLoop.var(), mapSchema, valueSchema, forBody);
        }

        nonEmptyMapBlock.invoke(JExpr.direct(ENCODER), "writeMapEnd");
    }

    private void processUnion(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            final Schema unionSchema,
            JBlock body) {
        processUnion(containerVariable, keyVariable, containerSchema, unionSchema, null, null, body);
    }

    private void processUnion(JVar containerVariable, final Schema containerSchema, final Schema unionSchema,
            JVar counterVariable,
            JBlock body) {
        processUnion(containerVariable, null, containerSchema, unionSchema, counterVariable, null, body);
    }

    private void processUnion(JVar containerVariable, final Schema containerSchema, final Schema unionSchema,
            final Schema.Field field,
            JBlock body) {
        processUnion(containerVariable, null, containerSchema, unionSchema, null, field, body);
    }

    private void processUnion(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            final Schema unionSchema, JVar counterVariable, Schema.Field field, JBlock body) {

        JVar unionVariable = null;
        if (Schema.Type.RECORD.equals(containerSchema.getType())) {
            unionVariable = body.decl(codeModel.ref(Object.class), getVariableName(field.name()), containerVariable
                    .invoke("get").arg(JExpr.lit(field.pos())));
        } else if (Schema.Type.ARRAY.equals(containerSchema.getType())) {
            unionVariable = body.decl(codeModel.ref(Object.class), getVariableName(containerSchema.getName()),
                    containerVariable.invoke("get").arg(counterVariable));
        } else if (Schema.Type.MAP.equals(containerSchema.getType())) {
            unionVariable = body.decl(codeModel.ref(Object.class), getVariableName(containerSchema.getName()),
                    containerVariable.invoke("get").arg(keyVariable));
        }

        JConditional ifBlock = null;
        for (Schema schemaOption : unionSchema.getTypes()) {
            // Special handling for null
            if (Schema.Type.NULL.equals(schemaOption.getType())) {
                JExpression condition = unionVariable.eq(JExpr._null());
                ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
                JBlock thenBlock = ifBlock._then();
                thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
                        .arg(JExpr.lit(unionSchema.getIndexNamed(schemaOption.getType().getName())));
                thenBlock.invoke(JExpr.direct(ENCODER), "writeNull");
                continue;
            }

            JClass optionClass = schemaAnalyzer.classFromSchema(schemaOption);
            JClass rawOptionClass = schemaAnalyzer.classFromSchema(schemaOption, true, true);
            JExpression condition = unionVariable._instanceof(rawOptionClass);
            if (useGenericTypes && SchemaAnalyzer.isNamedClass(schemaOption)) {
                condition = condition.cand(JExpr.invoke(JExpr.lit(schemaOption.getFullName()), "equals")
                        .arg(JExpr.invoke(JExpr.cast(optionClass, unionVariable), "getSchema").invoke("getFullName")));
            }
            ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
            JBlock thenBlock = ifBlock._then();
            thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
                    .arg(JExpr.lit(unionSchema.getIndexNamed(schemaOption.getFullName())));
            JVar optionVar = thenBlock
                    .decl(optionClass, getVariableName(schemaOption.getName()), JExpr.cast(optionClass, unionVariable));

            switch (schemaOption.getType()) {

            case RECORD:
                processRecord(schemaOption, optionVar, thenBlock);
                break;
            case ENUM:
                processEnum(optionVar, schemaOption, thenBlock);
                break;
            case ARRAY:
                processArray(schemaOption, optionVar, thenBlock);
                break;
            case MAP:
                processMap(schemaOption, optionVar, thenBlock);
                break;
            case FIXED:
                processFixed(optionVar, schemaOption, thenBlock);
                break;
            case UNION:
            case NULL:
                throw new FastSerializerGeneratorException("Incorrect union subschema processing: " + schemaOption);
            default:
                processPrimitive(unionVariable, schemaOption, thenBlock);
            }
        }
    }

    private void processFixed(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            JBlock body) {
        processFixed(containerVariable, keyVariable, containerSchema, null, null, body);
    }

    private void processFixed(JVar containerVariable, final Schema containerSchema,
            final Schema.Field readerField, JBlock body) {
        processFixed(containerVariable, null, containerSchema, null, readerField, body);
    }

    private void processFixed(JVar containerVariable, final Schema containerSchema, JVar counterVariable,
            JBlock body) {
        processFixed(containerVariable, null, containerSchema, counterVariable, null, body);
    }

    private void processFixed(JVar containerVariable, final Schema containerSchema, JBlock body) {
        processFixed(containerVariable, null, containerSchema, null, null, body);
    }

    private void processFixed(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            JVar counterVariable, final Schema.Field field, JBlock body) {

        if (Schema.Type.RECORD.equals(containerSchema.getType())) {
            final JClass fixedClass = useGenericTypes ?
                    codeModel.ref(GenericData.Fixed.class) :
                    codeModel.ref(field.schema().getFullName());

            body.invoke(JExpr.direct(ENCODER), "writeFixed").arg(JExpr
                    .invoke(JExpr.cast(fixedClass, containerVariable.invoke("get").arg(JExpr.lit(field.pos()))),
                            "bytes"));
        } else if (Schema.Type.ARRAY.equals(containerSchema.getType())) {
            final JClass fixedClass = useGenericTypes ?
                    codeModel.ref(GenericData.Fixed.class) :
                    codeModel.ref(containerSchema.getElementType().getFullName());

            body.invoke(JExpr.direct(ENCODER), "writeFixed").arg(JExpr
                    .invoke(JExpr.cast(fixedClass, containerVariable.invoke("get").arg(counterVariable)), "bytes"));
        } else if (Schema.Type.MAP.equals(containerSchema.getType())) {
            final JClass fixedClass = useGenericTypes ?
                    codeModel.ref(GenericData.Fixed.class) :
                    codeModel.ref(containerSchema.getValueType().getFullName());
            body.invoke(JExpr.direct(ENCODER), "writeFixed").arg(JExpr
                    .invoke(JExpr.cast(fixedClass, containerVariable.invoke("get").arg(keyVariable)), "bytes"));
        } else if (Schema.Type.FIXED.equals(containerSchema.getType())) {
            body.invoke(JExpr.direct(ENCODER), "writeFixed").arg(containerVariable.invoke("bytes"));
        }
    }

    private void processEnum(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            JBlock body) {
        processEnum(containerVariable, keyVariable, containerSchema, null, null, body);
    }

    private void processEnum(JVar containerVariable, final Schema containerSchema,
            final Schema.Field readerField, JBlock body) {
        processEnum(containerVariable, null, containerSchema, null, readerField, body);
    }

    private void processEnum(JVar containerVariable, final Schema containerSchema, JVar counterVariable,
            JBlock body) {
        processEnum(containerVariable, null, containerSchema, counterVariable, null, body);
    }

    private void processEnum(JVar containerVariable, final Schema containerSchema, JBlock body) {
        processEnum(containerVariable, null, containerSchema, null, null, body);
    }

    private void processEnum(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            JVar counterVariable, final Schema.Field field, JBlock body) {

        if (Schema.Type.RECORD.equals(containerSchema.getType())) {
            final JClass enumClass = useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                    : codeModel.ref(field.schema().getFullName());

            if (useGenericTypes) {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(
                        JExpr.invoke(JExpr.invoke(
                                JExpr.cast(enumClass, containerVariable.invoke("get").arg(JExpr.lit(field.pos()))),
                                "getSchema"), "getEnumOrdinal")
                                .arg(JExpr.invoke(
                                        JExpr.cast(enumClass,
                                                containerVariable.invoke("get").arg(JExpr.lit(field.pos()))),
                                        "toString")));
            } else {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(
                        JExpr.invoke(
                                JExpr.cast(enumClass, containerVariable.invoke("get").arg(JExpr.lit(field.pos()))),
                                "ordinal"));
            }
        } else if (Schema.Type.ARRAY.equals(containerSchema.getType())) {
            final JClass enumClass = useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                    : codeModel.ref(containerSchema.getElementType().getFullName());

            if (useGenericTypes) {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(
                        JExpr.invoke(
                                JExpr.invoke(
                                        JExpr.cast(enumClass, containerVariable.invoke("get").arg(counterVariable)),
                                        "getSchema"),
                                "getEnumOrdinal")
                                .arg(JExpr.invoke(
                                        JExpr.cast(enumClass, containerVariable.invoke("get").arg(counterVariable)),
                                        "toString")));
            } else {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(
                        JExpr.invoke(JExpr.cast(enumClass, containerVariable.invoke("get").arg(counterVariable)),
                                "ordinal"));
            }
        } else if (Schema.Type.MAP.equals(containerSchema.getType())) {
            final JClass enumClass = useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                    : codeModel.ref(containerSchema.getValueType().getFullName());

            if (useGenericTypes) {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(
                        JExpr.invoke(
                                JExpr.invoke(JExpr.cast(enumClass, containerVariable.invoke("get").arg(keyVariable)),
                                        "getSchema"),
                                "getEnumOrdinal")
                                .arg(JExpr.invoke(
                                        JExpr.cast(enumClass, containerVariable.invoke("get").arg(keyVariable)),
                                        "toString")));
            } else {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(
                        JExpr.invoke(JExpr.cast(enumClass, containerVariable.invoke("get").arg(keyVariable)),
                                "ordinal"));
            }
        } else if (Schema.Type.ENUM.equals(containerSchema.getType())) {
            if (useGenericTypes) {
                body.invoke(JExpr.direct(ENCODER), "writeEnum")
                        .arg(JExpr.invoke(containerVariable.invoke("getSchema"), "getEnumOrdinal")
                                .arg(containerVariable.invoke("toString")));
            } else {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(containerVariable.invoke("ordinal"));
            }
        }
    }

    private void processPrimitive(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            final Schema primitiveSchema, JBlock body) {
        processPrimitive(containerVariable, keyVariable, null, containerSchema, primitiveSchema, null, body);
    }

    private void processPrimitive(JVar containerVariable, final Schema containerSchema, final Schema primitiveSchema,
            JVar counterVariable, JBlock body) {
        processPrimitive(containerVariable, null, counterVariable, containerSchema, primitiveSchema, null, body);
    }

    private void processPrimitive(JVar containerVariable, final Schema containerSchema, final Schema primitiveSchema,
            final Schema.Field field, JBlock body) {
        processPrimitive(containerVariable, null, null, containerSchema, primitiveSchema, field, body);
    }

    private void processPrimitive(JVar containerVariable, final Schema primitiveSchema, JBlock body) {
        processPrimitive(containerVariable, null, null, primitiveSchema, primitiveSchema, null, body);
    }

    private void processPrimitive(JVar containerVariable, JVar keyVariable, JVar counterVariable,
            final Schema containerSchema, final Schema primitiveSchema, final Schema.Field field, JBlock body) {
        String writeFunction = null;
        Class<?> castType = null;
        String valueJavaClassName = primitiveSchema.getProp("java-class");
        if (Schema.Type.BOOLEAN.equals(primitiveSchema.getType())) {
            writeFunction = "writeBoolean";
            castType = Boolean.class;
        } else if (Schema.Type.INT.equals(primitiveSchema.getType())) {
            writeFunction = "writeInt";
            castType = Integer.class;
        } else if (Schema.Type.LONG.equals(primitiveSchema.getType())) {
            writeFunction = "writeLong";
            castType = Long.class;
        } else if (Schema.Type.STRING.equals(primitiveSchema.getType())) {
            writeFunction = "writeString";
            castType = String.class;
        } else if (Schema.Type.DOUBLE.equals(primitiveSchema.getType())) {
            writeFunction = "writeDouble";
            castType = Double.class;
        } else if (Schema.Type.FLOAT.equals(primitiveSchema.getType())) {
            writeFunction = "writeFloat";
            castType = Float.class;
        } else if (Schema.Type.BYTES.equals(primitiveSchema.getType())) {
            writeFunction = "writeBytes";
            castType = ByteBuffer.class;
        }

        if (writeFunction == null) {
            throw new FastSerializerGeneratorException(
                    "Unsupported primitive schema of type: " + primitiveSchema.getType());
        }

        JExpression primitiveValueExpression;

        if (Schema.Type.RECORD.equals(containerSchema.getType())) {
            primitiveValueExpression = containerVariable.invoke("get").arg(JExpr.lit(field.pos()));
        } else if (Schema.Type.ARRAY.equals(containerSchema.getType())) {
            primitiveValueExpression = containerVariable.invoke("get").arg(counterVariable);
        } else if (Schema.Type.MAP.equals(containerSchema.getType())) {
            primitiveValueExpression = containerVariable.invoke("get").arg(keyVariable);
        } else {
            primitiveValueExpression = containerVariable;
        }

        if (valueJavaClassName != null) {
            primitiveValueExpression = primitiveValueExpression.invoke("toString");
        }

        body.invoke(JExpr.direct(ENCODER), writeFunction)
                .arg(JExpr.cast(codeModel.ref(castType), primitiveValueExpression));
    }

    private JVar declareContainerVariableForSchemaInBlock(final String name, final Schema schema, JBlock block) {
        if (Schema.Type.ARRAY.equals(schema.getType())) {
            return block.decl(codeModel.ref(List.class)
                    .narrow(schemaAnalyzer.elementClassFromArraySchema(schema)), getVariableName(name), JExpr._null());
        } else if (Schema.Type.MAP.equals(schema.getType())) {
            return block.decl(codeModel.ref(Map.class)
                            .narrow(schemaAnalyzer.keyClassFromMapSchema(schema),
                                    schemaAnalyzer.elementClassFromMapSchema(schema)),
                    getVariableName(name), JExpr._null());
        } else if (Schema.Type.RECORD.equals(schema.getType())) {
            return block.decl((useGenericTypes ?
                    codeModel.ref(GenericData.Record.class) :
                    codeModel.ref(schema.getFullName())), getVariableName(name), JExpr._null());
        } else {
            throw new FastDeserializerGeneratorException("Incorrect container variable: " + schema.getType().getName());
        }
    }

    private boolean doesNotContainMethod(final Schema schema) {
        return Schema.Type.RECORD.equals(schema.getType())
                && !serializeMethodMap.containsKey(schema.getFullName());
    }

    private JMethod getMethod(final Schema schema) {
        if (Schema.Type.RECORD.equals(schema.getType())) {
            if (!doesNotContainMethod(schema)) {
                return serializeMethodMap.get(schema.getFullName());
            }
            throw new FastSerializerGeneratorException("No method for schema: " + schema.getFullName());
        }
        throw new FastSerializerGeneratorException("No method for schema type: " + schema.getType());
    }

    private JMethod createMethod(final Schema schema) {
        if (Schema.Type.RECORD.equals(schema.getType())) {
            if (doesNotContainMethod(schema)) {
                JMethod method = serializerClass.method(JMod.PUBLIC, codeModel.VOID,
                        "serialize" + schema.getName() + nextRandomInt());
                method._throws(IOException.class);
                method.param(
                        useGenericTypes ? codeModel.ref(GenericData.Record.class) : codeModel.ref(schema.getFullName()),
                        "data");
                method.param(Encoder.class, ENCODER);

                method.annotate(SuppressWarnings.class).param("value", "unchecked");

                serializeMethodMap.put(schema.getFullName(), method);

                return method;
            } else {
                throw new FastSerializerGeneratorException("Method already exists for: " + schema.getFullName());
            }
        }
        throw new FastSerializerGeneratorException("No method for schema type: " + schema.getType());
    }

}
