package cascading.avro.coercions;

import cascading.CascadingException;
import cascading.tuple.Tuple;
import cascading.tuple.type.CoercibleType;
import cascading.util.Util;
import org.apache.avro.Schema;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;


public class AvroMapCoercion implements CoercibleType {

  private Schema values;

  public AvroMapCoercion(Schema values) {
    this.values = values;
  }


  public Schema getValuesType() {
    return values;
  }

  /**
   * @param value
   * @return
   */
  @Override
  public Object canonical(Object value) {
    if (value == null)
      return null;

    Class from = value.getClass();


    if (from == Map.class)
      return (Map) value;

    if (from == Tuple.class) {
      Tuple tuple = (Tuple) value;
      Map<String, Object> map = new HashMap<String, Object>();
      for (int i = 0; i < tuple.size(); i += 2) map.put(tuple.getString(i), tuple.getObject(i + 1));
      return map;
    }
    throw new CascadingException("unknown type coercion requested from: " + Util.getTypeName(from));
  }

  /**
   * @param value
   * @param to
   * @return
   */
  @Override
  public Object coerce(Object value, Type to) {

    if (value == null)
      return null;

    Class from = value.getClass();

    if (from != Map.class)
      throw new IllegalStateException("From value is not a Map");

    if (to == Map.class)
      return (Map) value;

    if (to == Tuple.class) {
      Map<String, Object> map = (Map<String, Object>) value;
      Tuple tuple = new Tuple();
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        tuple.addString(entry.getKey().toString());
        tuple.add(entry.getValue());
      }
      return tuple;
    }
    throw new CascadingException("unknown type coercion requested, from: " + Util.getTypeName(from) + " to: " + Util.getTypeName(to));
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeUTF(this.values.toString());
  }

  private void readObject(java.io.ObjectInputStream in)
      throws IOException {
    this.values = (new Schema.Parser()).parse(in.readUTF());
  }
}
