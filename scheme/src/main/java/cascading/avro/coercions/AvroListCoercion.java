package cascading.avro.coercions;

import cascading.CascadingException;
import cascading.tuple.Tuple;
import cascading.tuple.type.CoercibleType;
import cascading.util.Util;
import org.apache.avro.Schema;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;


public class AvroListCoercion implements CoercibleType {

  private Schema element;

  public AvroListCoercion(Schema element) {
    this.element = element;
  }


  public Schema getElementType() {
    return element;
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


    if (from == List.class)
      return (List) value;

    if (from == Tuple.class) {
      Tuple tuple = (Tuple) value;
      List list = new ArrayList();
      for (Object obj : tuple) list.add(obj);
      return list;
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

    if (from != List.class)
      throw new IllegalStateException("From value is not a List");

    if (to == List.class)
      return (List) value;

    if (to == Tuple.class) {
      List list = (List) value;
      Tuple tuple = new Tuple();
      tuple.addAll(list);
      return tuple;
    }
    throw new CascadingException("unknown type coercion requested, from: " + Util.getTypeName(from) + " to: " + Util.getTypeName(to));
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeUTF(this.element.toString());
  }

  private void readObject(java.io.ObjectInputStream in)
      throws IOException {
    this.element = (new Schema.Parser()).parse(in.readUTF());
  }
}
