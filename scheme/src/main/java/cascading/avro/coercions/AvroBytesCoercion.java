package cascading.avro.coercions;

import cascading.CascadingException;
import cascading.tuple.type.CoercibleType;
import cascading.util.Util;
import org.apache.hadoop.io.BytesWritable;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;

public class AvroBytesCoercion implements CoercibleType {

  /**
   * @param value
   * @return
   */
  @Override
  public Object canonical(Object value) {

    if (value == null) return value;

    Class from = value.getClass();

    if (from == BytesWritable.class)
      return (BytesWritable) value;

    if (from == ByteBuffer.class)
      return new BytesWritable(((ByteBuffer) value).array());

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

    if (from != BytesWritable.class)
      throw new IllegalStateException("From value is not a BytesWritable");

    if (to == BytesWritable.class)
      return (BytesWritable) value;

    if (to == ByteBuffer.class)
      return ByteBuffer.wrap(((BytesWritable) value).getBytes());

    throw new CascadingException("unknown type coercion requested, from: " + Util.getTypeName(from) + " to: " + Util.getTypeName(to));
  }
}
