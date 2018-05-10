package com.rtbhouse.utils.avro;

import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilderException;
import org.apache.avro.generic.GenericData;

import java.nio.ByteBuffer;
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
        if (!schema.getType().equals(Schema.Type.MAP)) {
            throw new FastAvroSchemaAnalyzeException("Map schema was expected, instead got:"
                    + schema.getType().getName());
        }
        String keyClassName = schema.getProp("java-key-class");
        if (keyClassName != null) {
            return codeModel.ref(keyClassName);
        } else {
            return codeModel.ref(String.class);
        }
    }

    JClass elementClassFromMapSchema(Schema schema) {
        if (!schema.getType().equals(Schema.Type.MAP)) {
            throw new FastAvroSchemaAnalyzeException("Map schema was expected, instead got:"
                    + schema.getType().getName());
        }

        return classFromSchema(schema.getValueType());
    }

    JClass elementClassFromArraySchema(Schema schema) {
        if (!Schema.Type.ARRAY.equals(schema.getType())) {
            throw new FastAvroSchemaAnalyzeException("Array schema was expected, instead got:"
                    + schema.getType().getName());
        }

        return classFromSchema(schema.getElementType());
    }

    JClass classFromUnionSchema(final Schema schema) {
        if (!Schema.Type.UNION.equals(schema.getType())) {
            throw new FastAvroSchemaAnalyzeException("Union schema was expected, instead got:"
                    + schema.getType().getName());
        }

        if (schema.getTypes().size() == 1) {
            return classFromSchema(schema.getTypes().get(0));
        }

        if (schema.getTypes().size() == 2) {
            if (Schema.Type.NULL.equals(schema.getTypes().get(0).getType())) {
                return classFromSchema(schema.getTypes().get(1));
            } else if (Schema.Type.NULL.equals(schema.getTypes().get(1).getType())) {
                return classFromSchema(schema.getTypes().get(0));
            }
        }

        return codeModel.ref(Object.class);
    }

    JClass classFromSchema(Schema schema) {
        return classFromSchema(schema, true, false);
    }

    JClass classFromSchema(Schema schema, boolean abstractType) {
        return classFromSchema(schema, abstractType, false);
    }

    JClass classFromSchema(Schema schema, boolean abstractType, boolean rawType) {
        JClass outputClass = null;

        switch (schema.getType()) {

        case RECORD:
            outputClass = useGenericTypes ? codeModel.ref(GenericData.Record.class) : codeModel.ref(schema.getFullName());
            break;

        case ARRAY:
            if (abstractType) {
                outputClass = codeModel.ref(List.class);
            } else {
                if (useGenericTypes) {
                    outputClass = codeModel.ref(GenericData.Array.class);
                } else {
                    outputClass = codeModel.ref(ArrayList.class);
                }
            }
            if (!rawType) {
                outputClass = outputClass.narrow(elementClassFromArraySchema(schema));
            }
            break;
        case MAP:
            if (!abstractType) {
                outputClass = codeModel.ref(HashMap.class);
            } else {
                outputClass = codeModel.ref(Map.class);
            }
            if (!rawType) {
                outputClass = outputClass.narrow(keyClassFromMapSchema(schema), elementClassFromMapSchema(schema));
            }
            break;
        case UNION:
            outputClass = classFromUnionSchema(schema);
            break;
        case ENUM:
            outputClass = useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class) : codeModel.ref(schema.getFullName());
            break;
        case FIXED:
            outputClass = useGenericTypes ? codeModel.ref(GenericData.Fixed.class) : codeModel.ref(schema.getFullName());
            break;
        case BOOLEAN:
            outputClass = codeModel.ref(Boolean.class);
            break;
        case DOUBLE:
            outputClass = codeModel.ref(Double.class);
            break;
        case FLOAT:
            outputClass = codeModel.ref(Float.class);
            break;
        case INT:
            outputClass = codeModel.ref(Integer.class);
            break;
        case LONG:
            outputClass = codeModel.ref(Long.class);
            break;
        case STRING:
            if (schema.getProp("java-class") != null) {
                outputClass = codeModel.ref(schema.getProp("java-class"));
            } else {
                outputClass = codeModel.ref(String.class);
            }
            break;
        case BYTES:
            outputClass = codeModel.ref(ByteBuffer.class);
            break;
        case NULL:
            throw new SchemaBuilderException("Asking for class for NULL type!");
        }

        return outputClass;
    }

    public static boolean isPrimitiveTypeUnion(Schema schema) {
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

    public static boolean isPrimitiveType(Schema schema) {
        switch (schema.getType()) {
        case RECORD:
        case ARRAY:
        case MAP:
        case ENUM:
        case FIXED:
            return false;
        default:
            return true;
        }
    }

    public static boolean isComplexType(Schema schema) {
        switch (schema.getType()) {
        case MAP:
        case RECORD:
        case ARRAY:
        case UNION:
            return true;
        default:
            return false;
        }
    }

    public static boolean isNamedClass(Schema schema) {
        switch (schema.getType()) {
        case RECORD:
        case ENUM:
        case FIXED:
            return true;
        default:
            return false;
        }
    }
}
