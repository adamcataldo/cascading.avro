/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.avro;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.BytesWritable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class AvroToCascading {

  public static Object[] parseRecord(Record record, Schema readerSchema) {

    Object[] result = new Object[readerSchema.getFields().size()];
    Schema writerSchema = record.getSchema();
    List<Field> schemaFields = readerSchema.getFields();
    for (int i = 0; i < schemaFields.size(); i++) {
      Field field = schemaFields.get(i);
      if (writerSchema.getField(field.name()) == null) {
        throw new AvroRuntimeException("Not a valid schema field: " + field.name());
      }
      Object obj = record.get(field.name());
      result[i] = fromAvro(obj, field.schema());

    }
    return result;
  }

  static Object fromAvro(Object obj, Schema schema) {
    switch (schema.getType()) {

      case UNION:
        return fromAvroUnion(obj, schema);

      case ARRAY:
        return fromAvroArray(obj, schema);

      case STRING:
      case ENUM:
        return obj.toString();

      case FIXED:
        return fromAvroFixed(obj);
      case BYTES:
        return fromAvroBytes((ByteBuffer) obj);

      case RECORD:
        return new AvroTuple((Record) obj);

      case MAP:
        return fromAvroMap(obj, schema);

      case NULL:
      case BOOLEAN:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
        return obj;

      default:
        throw new RuntimeException("Can't convert from type " + schema.getType().toString());

    }
  }

  private static Object fromAvroFixed(Object obj) {
    Fixed fixed = (Fixed) obj;
    return new BytesWritable(fixed.bytes());
  }

  @SuppressWarnings("unchecked")
  private static Object fromAvroMap(Object obj, Schema schema) {

    Map<String, Object> convertedMap = new HashMap<String, Object>();
    for (Map.Entry<Utf8, Object> e : ((Map<Utf8, Object>) obj).entrySet()) {
      convertedMap.put(e.getKey().toString(), fromAvro(e.getValue(), schema.getValueType()));
    }
    return convertedMap;
  }

  private static Object fromAvroBytes(ByteBuffer val) {
    return new BytesWritable(val.array());
  }

  private static Object fromAvroArray(Object obj, Schema schema) {
    List<Object> array = new ArrayList<Object>();
    for (Object element : (GenericData.Array) obj) {
      array.add(fromAvro(element, schema.getElementType()));
    }
    return array;
  }

  private static Object fromAvroUnion(Object obj, Schema schema) {
    List<Schema> types = schema.getTypes();
    if (types.size() < 1) {
      throw new AvroRuntimeException("Union has no types");
    }
    if (types.size() == 1) {
      return fromAvro(obj, types.get(0));
    } else if (types.size() > 2) {
      throw new AvroRuntimeException("Unions may only consist of a concrete type and null in cascading.avro");
    } else if (!types.get(0).getType().equals(Type.NULL) && !types.get(1).getType().equals(Type.NULL)) {
      throw new AvroRuntimeException("Unions may only consist of a concrete type and null in cascading.avro");
    } else {
      Integer concreteIndex = (types.get(0).getType() == Type.NULL) ? 1 : 0;
      return fromAvro(obj, types.get(concreteIndex));
    }
  }

}
