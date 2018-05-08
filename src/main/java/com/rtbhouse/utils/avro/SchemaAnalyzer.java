package com.rtbhouse.utils.avro;

import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SchemaAnalyzer {
    private JCodeModel codeModel;
    private boolean useGenericTypes;

    SchemaAnalyzer(JCodeModel codeModel, boolean useGenericTypes) {
        this.codeModel = codeModel;
        this.useGenericTypes = useGenericTypes;
    }

    JClass keyClassFromMapSchema(Schema schema) {
        String keyClassName = schema.getProp("java-key-class");
        if (keyClassName != null) {
            try {
                return codeModel.ref(Class.forName(keyClassName));
            } catch (ClassNotFoundException e) {
                throw new FastSerializerGeneratorException("Unknown key class" + keyClassName);
            }
        } else {
            return codeModel.ref(String.class);
        }
    }

    JClass elementClassFromMapSchema(Schema schema) {
        if (!schema.getType().equals(Schema.Type.MAP)) {
            throw new FastDeserializerGeneratorException("Map schema was expected, instead got:"
                    + schema.getType().getName());
        }

        Schema.Type elementType = schema.getValueType().getType();

        if (Schema.Type.RECORD.equals(elementType)) {
            return useGenericTypes ? codeModel.ref(GenericData.Record.class)
                    : codeModel.ref(schema.getValueType()
                            .getFullName());
        } else if (Schema.Type.ARRAY.equals(elementType)) {
            return codeModel.ref(List.class).narrow(elementClassFromArraySchema(schema.getValueType()));
        } else if (Schema.Type.MAP.equals(elementType)) {
            return codeModel.ref(Map.class).narrow(String.class)
                    .narrow(elementClassFromArraySchema(schema.getValueType()));
        } else if (Schema.Type.ENUM.equals(elementType)) {
            return useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                    : codeModel.ref(schema.getValueType()
                            .getFullName());
        } else if (Schema.Type.FIXED.equals(elementType)) {
            return useGenericTypes ? codeModel.ref(GenericData.Fixed.class)
                    : codeModel.ref(schema.getValueType()
                            .getFullName());
        } else if (Schema.Type.UNION.equals(elementType)) {
            return classFromUnionSchema(schema.getValueType());
        }

        try {
            String primitiveClassName;
            switch (schema.getValueType().getName()) {
            case "int":
                primitiveClassName = "java.lang.Integer";
                break;
            case "bytes":
                primitiveClassName = "java.nio.ByteBuffer";
                break;
            case "string":
                if (schema.getValueType().getProp("java-class") != null) {
                    primitiveClassName = schema.getValueType().getProp("java-class");
                } else {
                    primitiveClassName = "java.lang.String";
                }
                break;
            default:
                primitiveClassName = "java.lang." + StringUtils.capitalize(StringUtils.lowerCase(schema
                        .getValueType().getName()));
            }
            return codeModel.ref(Class.forName(primitiveClassName));
        } catch (ReflectiveOperationException e) {
            throw new FastDeserializerGeneratorException("Unknown type: " + schema
                    .getValueType().getName(), e);
        }
    }

    JClass elementClassFromArraySchema(Schema schema) {
        if (!Schema.Type.ARRAY.equals(schema.getType())) {
            throw new FastDeserializerGeneratorException("Array schema was expected, instead got:"
                    + schema.getType().getName());
        }

        Schema.Type elementType = schema.getElementType().getType();

        if (Schema.Type.RECORD.equals(elementType)) {
            return useGenericTypes ? codeModel.ref(GenericData.Record.class)
                    : codeModel.ref(schema.getElementType()
                            .getFullName());
        } else if (Schema.Type.ARRAY.equals(elementType)) {
            return codeModel.ref(List.class).narrow(
                    elementClassFromArraySchema(schema.getElementType()));
        } else if (Schema.Type.MAP.equals(elementType)) {
            return codeModel.ref(Map.class).narrow(keyClassFromMapSchema(schema))
                    .narrow(elementClassFromMapSchema(schema.getElementType()));
        } else if (Schema.Type.ENUM.equals(elementType)) {
            return useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                    : codeModel.ref(schema
                            .getElementType().getFullName());
        } else if (Schema.Type.FIXED.equals(elementType)) {
            return useGenericTypes ? codeModel.ref(GenericData.Fixed.class)
                    : codeModel.ref(schema
                            .getElementType().getFullName());
        } else if (Schema.Type.UNION.equals(elementType)) {
            return classFromUnionSchema(schema.getElementType());
        }

        try {
            String primitiveClassName;
            switch (schema.getElementType().getName()) {
            case "int":
                primitiveClassName = "java.lang.Integer";
                break;
            case "bytes":
                primitiveClassName = "java.nio.ByteBuffer";
                break;
            case "string":
                if (schema.getElementType().getProp("java-class") != null) {
                    primitiveClassName = schema.getElementType().getProp("java-class");
                } else {
                    primitiveClassName = "java.lang.String";
                }
                break;
            default:
                primitiveClassName = "java.lang." + StringUtils.capitalize(StringUtils.lowerCase(schema
                        .getElementType().getName()));
            }
            return codeModel.ref(Class.forName(primitiveClassName));
        } catch (ReflectiveOperationException e) {
            throw new FastDeserializerGeneratorException("Unknown type: " + schema
                    .getElementType().getName(), e);
        }
    }

    JClass classFromUnionSchema(final Schema schema) {
        if (!Schema.Type.UNION.equals(schema.getType())) {
            throw new FastDeserializerGeneratorException("Union schema was expected, instead got:"
                    + schema.getType().getName());
        }

        if (schema.getTypes().size() > 2) {
            return codeModel.ref(Object.class);
        }

        Schema unionSchema = null;
        if (schema.getTypes().size() == 2) {
            if (Schema.Type.NULL.equals(schema.getTypes().get(0).getType())) {
                unionSchema = schema.getTypes().get(1);
            } else if (Schema.Type.NULL.equals(schema.getTypes().get(1).getType())) {
                unionSchema = schema.getTypes().get(0);
            } else {
                return codeModel.ref(Object.class);
            }
        }

        if (unionSchema != null) {
            if (Schema.Type.RECORD.equals(unionSchema.getType())) {
                return useGenericTypes ? codeModel.ref(GenericData.Record.class)
                        : codeModel.ref(unionSchema
                                .getFullName());
            } else if (Schema.Type.ARRAY.equals(unionSchema.getType())) {
                return codeModel.ref(List.class).narrow(elementClassFromArraySchema(unionSchema));
            } else if (Schema.Type.MAP.equals(unionSchema.getType())) {
                return codeModel.ref(Map.class).narrow(String.class)
                        .narrow(elementClassFromArraySchema(unionSchema));
            } else if (Schema.Type.ENUM.equals(unionSchema.getType())) {
                return useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                        : codeModel.ref(unionSchema
                                .getFullName());
            } else if (Schema.Type.FIXED.equals(unionSchema.getType())) {
                return useGenericTypes ? codeModel.ref(GenericData.Fixed.class)
                        : codeModel.ref(unionSchema.getFullName());
            }

            throw new FastDeserializerGeneratorException("Could not determine union element schema");
        } else {
            throw new FastDeserializerGeneratorException("Could not determine union element schema");
        }
    }

    boolean isConatinerType(Schema schema) {
        if (Schema.Type.RECORD.equals(schema.getType())) {
            return true;
        } else if (Schema.Type.ARRAY.equals(schema.getType())) {
            return true;
        } else if (Schema.Type.MAP.equals(schema.getType())) {
            return true;
        } else {
            return true;
        }
    }

    boolean isPrimitiveType(Schema schema) {
        if (Schema.Type.RECORD.equals(schema.getType())) {
            return false;
        } else if (Schema.Type.ARRAY.equals(schema.getType())) {
            return false;
        } else if (Schema.Type.MAP.equals(schema.getType())) {
            return false;
        } else if (Schema.Type.ENUM.equals(schema.getType())) {
            return false;
        } else if (Schema.Type.FIXED.equals(schema.getType())) {
            return false;
        } else {
            return true;
        }
    }

    boolean isPrimitiveTypeUnion(final Schema schema) {
        if (!Schema.Type.UNION.equals(schema.getType())) {
            return false;
        }
        for (Schema unionOptionSchema : schema.getTypes()) {
            if (!isPrimitiveType(unionOptionSchema)) {
                return false;
            }
        }
        return true;
    }

    JClass classFromSchema(Schema schema) {
        return classFromSchema(schema, false);
    }

    JClass classFromSchema(Schema schema, boolean concreteType) {
        JClass outputClass;
        if (Schema.Type.RECORD.equals(schema.getType())) {
            if (useGenericTypes) {
                outputClass = codeModel.ref(GenericData.Record.class);
            } else {
                outputClass = codeModel.ref(schema.getFullName());
            }
        } else if (Schema.Type.ARRAY.equals(schema.getType())) {
            if (useGenericTypes) {
                outputClass = codeModel.ref(GenericData.Array.class)
                        .narrow(elementClassFromArraySchema(schema));
            } else {
                Class<? extends List> listClass;
                if (concreteType) {
                    listClass = ArrayList.class;
                } else {
                    listClass = List.class;
                }
                outputClass = codeModel.ref(listClass)
                        .narrow(elementClassFromArraySchema(schema));
            }
        } else if (Schema.Type.MAP.equals(schema.getType())) {
            Class<? extends Map> mapClass;
            if (concreteType) {
                mapClass = HashMap.class;
            } else {
                mapClass = Map.class;
            }
            outputClass = codeModel.ref(mapClass)
                    .narrow(keyClassFromMapSchema(schema),
                            elementClassFromMapSchema(schema));
        } else if (Schema.Type.NULL.equals(schema.getType())) {
            return null;
        } else {
            try {
                String primitiveClassName;
                switch (schema.getName()) {
                case "int":
                    primitiveClassName = "java.lang.Integer";
                    break;
                case "bytes":
                    primitiveClassName = "java.nio.ByteBuffer";
                    break;
                case "string":
                    if (schema.getProp("java-class") != null) {
                        primitiveClassName = schema.getProp("java-class");
                    } else {
                        primitiveClassName = "java.lang.String";
                    }
                    break;
                default:
                    primitiveClassName = "java.lang."
                            + StringUtils.capitalize(StringUtils.lowerCase(schema.getName()));
                }
                outputClass = codeModel.ref(Class.forName(primitiveClassName));
            } catch (ReflectiveOperationException e) {
                throw new FastDeserializerGeneratorException("unknown type: " + schema.getName(), e);
            }
        }
        return outputClass;
    }

}
