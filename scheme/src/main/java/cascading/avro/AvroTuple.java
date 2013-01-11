package cascading.avro;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleException;
import cascading.tuple.util.TupleViews;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * This is a thin wrapper on an incoming Avro object. Cascading is now using Tuple views so we can get away with
 * just providing adapters for the Tuple interface rather than doing any real conversions at read time.
 */
public class AvroTuple extends Tuple {

  private final GenericData.Record record;
  private final Schema schema;


  public AvroTuple(GenericData.Record inputRecord) {
    record = inputRecord;
    schema = inputRecord.getSchema();
  }

  /**
   * Method get returns the element at the given position of the underlying Record. A conversion
   * to the proper cascading type is also performed.
   *
   * @param pos of type int
   * @return Comparable
   */
  @Override
  public Object getObject(int pos) {
    return AvroToCascading.fromAvro(record.get(pos), schema.getFields().get(pos).schema());
  }



  @Override
  public int[] getPos(Fields declarator, Fields selector) {
    if (!declarator.isUnknown() && schema.getFields().size() != declarator.size())
      throw new TupleException("field declaration: " + declarator.print() + ", does not match tuple: " + print());

    return declarator.getPos(selector);
  }


  @Override
  public Tuple get(int[] pos) {
    if (pos == null || pos.length == 0)
      return this;

    return TupleViews.createNarrow(pos, this);
  }

  /**
   * Method is the inverse of {@link #remove(int[])}.
   *
   * @param pos of type int[]
   * @return Tuple
   */
  @Override
  public Tuple leave( int[] pos ) {
    throw new UnsupportedOperationException("Leave is not supported on AvroTuple");
  }

  /**
   * Method clear empties this Tuple instance. A subsequent call to {@link #size()} will return zero ({@code 0}).
   */
  @Override
  public void clear() {
    throw new UnsupportedOperationException("clear is not supported on AvroTuple");
  }

  /**
   * Method add is not supported for AvroTuples.
   *
   * @param value of type Comparable
   */
  @Override
  public void add(Comparable value) {
    throw new UnsupportedOperationException("Add not supported on AvroTuple");

  }

  /**
   * Method add is not supported for AvroTuples.
   *
   * @param value of type Object
   */
  @Override
  public void add(Object value) {
    throw new UnsupportedOperationException("Add not supported on AvroTuple");
  }

  /**
   * Method addBoolean is not supported for AvroTuples.
   *
   * @param value of type boolean
   */
  @Override
  public void addBoolean(boolean value) {
    throw new UnsupportedOperationException("Add not supported on AvroTuple");
  }

  /**
   * Method addShort is not supported for AvroTuples.
   *
   * @param value of type short
   */
  @Override
  public void addShort(short value) {
    throw new UnsupportedOperationException("Add not supported on AvroTuple");
  }

  /**
   * Method addInteger is not supported for AvroTuples.
   *
   * @param value of type int
   */
  @Override
  public void addInteger(int value) {
    throw new UnsupportedOperationException("Add not supported on AvroTuple");
  }

  /**
   * Method addLong is not supported for AvroTuples.
   *
   * @param value of type long
   */
  @Override
  public void addLong(long value) {
    throw new UnsupportedOperationException("Add not supported on AvroTuple");
  }

  /**
   * Method addFloat is not supported for AvroTuples.
   *
   * @param value of type float
   */
  @Override
  public void addFloat(float value) {
    throw new UnsupportedOperationException("Add not supported on AvroTuple");
  }

  /**
   * Method addDouble is not supported for AvroTuples.
   *
   * @param value of type double
   */
  @Override
  public void addDouble(double value) {
    throw new UnsupportedOperationException("Add not supported on AvroTuple");
  }

  /**
   * Method addString is not supported for AvroTuples.
   *
   * @param value of type String
   */
  @Override
  public void addString(String value) {
    throw new UnsupportedOperationException("Add not supported on AvroTuple");
  }

  /**
   * Method addAll is not supported for AvroTuples.
   *
   * @param values of type Object...
   */
  @Override
  public void addAll(Object... values) {
    throw new UnsupportedOperationException("Add not supported on AvroTuple");
  }

  /**
   * Method addAll is not supported for AvroTuples.
   *
   * @param tuple of type Tuple
   */
  @Override
  public void addAll(Tuple tuple) {
    throw new UnsupportedOperationException("Add not supported on AvroTuple");
  }


  /**
   * Method set sets the given value in the underlying record to the given index position in this instance.
   * A conversion to the proper avro type is performed.
   *
   * @param pos of type int
   * @param val of type Object
   */
  @Override
  public void set(int pos, Object val) {
    record.put(pos, CascadingToAvro.toAvro(val, schema.getFields().get(pos).schema()));
  }


  /**
   * Method setAll sets each element value of the given Tuple instance into the corresponding
   * position of the underlying Avro record of this instance.
   *
   * @param tuple of type Tuple
   */
  @Override
  public void setAll(Tuple tuple) {
    verifyModifiable();

    if( tuple == null )
      return;

    for( int i = 0; i < tuple.size(); i++ )
      set(i, tuple.getObject(i));
  }



  /**
   * Method setAll sets each element value of the given Tuple instances underlying Avro record of this instance.
   *
   * All given tuple instances after the first will be offset by the length of the prior tuple instances.
   *
   * @param tuples of type Tuple[]
   */
  @Override
  public void setAll(Tuple... tuples) {
    verifyModifiable();

    if( tuples.length == 0 )
      return;

    int pos = 0;
    for( int i = 0; i < tuples.length; i++ )
    {
      Tuple tuple = tuples[ i ];

      if( tuple == null ) // being defensive
        continue;

      for( int j = 0; j < tuple.size(); j++ )
        set(pos++, tuple.getObject(j));
    }
  }



  /**
   * Method setBoolean sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type boolean
   */
  @Override
  public void setBoolean(int index, boolean value) {
    verifyModifiable();

    set(index, value);
  }

  /**
   * Method setShort sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type short
   */
  @Override
  public void setShort(int index, short value) {
    verifyModifiable();

    set(index, value);
  }

  /**
   * Method setInteger sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type int
   */
  @Override
  public void setInteger(int index, int value) {
    verifyModifiable();

    set(index, value);
  }

  /**
   * Method setLong sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type long
   */
  @Override
  public void setLong(int index, long value) {
    verifyModifiable();

    set(index, value);
  }

  /**
   * Method setFloat sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type float
   */
  @Override
  public void setFloat(int index, float value) {
    verifyModifiable();

    set(index, value);
  }

  /**
   * Method setDouble sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type double
   */
  @Override
  public void setDouble(int index, double value) {
    verifyModifiable();

    set(index, value);
  }

  /**
   * Method setString sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type String
   */
  @Override
  public void setString(int index, String value) {
    verifyModifiable();

    set(index, value);
  }




  /**
   * Method put places the values of the given tuple into the positions specified by the fields argument. The declarator
   * Fields value declares the fields in this Tuple instance.
   *
   * @param declarator of type Fields
   * @param fields     of type Fields
   * @param tuple      of type Tuple
   */
  @Override
  public void put(Fields declarator, Fields fields, Tuple tuple) {
    verifyModifiable();

    int[] pos = getPos( declarator, fields );

    for( int i = 0; i < pos.length; i++ )
      set(pos[i], tuple.getObject(i));
  }

  /**
   * Method remove removes the values specified by the given pos array and returns a new Tuple containing the
   * removed values.
   *
   * @param pos of type int[]
   * @return Tuple
   */
  @Override
  public Tuple remove(int[] pos) {
    verifyModifiable();
    throw new UnsupportedOperationException("Remove is not supported for AvroTuples");
  }


  /**
   * Method append appends all the values of the given Tuple instances to a copy of this instance.
   *
   * @param tuples of type Tuple
   * @return Tuple
   */
  @Override
  public Tuple append(Tuple... tuples) {
    Tuple[] appendedTuples = new Tuple[tuples.length+1];
    appendedTuples[0] = this;
    for (int i = 0; i < tuples.length; i++) appendedTuples[i+1] = tuples[i];
    return TupleViews.createComposite(appendedTuples);
  }

  /**
   * Method compareTo compares this Tuple to the given Tuple instance.
   *
   * @param other of type Tuple
   * @return int
   */
  @Override
  public int compareTo(Tuple other) {
    if( other == null || other.isEmpty() )
      return 1;

    if( other.size() != this.size() )
      return this.size() - other.size();

    if (other instanceof AvroTuple)
      return record.compareTo(((AvroTuple) other).getRecord());

    for( int i = 0; i < this.size(); i++ )
    {

      Object lhs =  this.getObject( i );
      Object rhs =  other.getObject( i );

      if( lhs == null && rhs == null )
        continue;

      if( lhs == null && rhs != null )
        return -1;
      else if( lhs != null && rhs == null )
        return 1;

      if (! (lhs instanceof Comparable && rhs instanceof Comparable))
        continue;

      if (!(lhs instanceof Comparable))
        return -1;

      if (!(rhs instanceof Comparable))
        return 1;


      int c = ((Comparable) lhs).compareTo(rhs); // guaranteed to not be null
      if( c != 0 )
        return c;
    }

    return 0;
  }


  @Override
  public boolean equals(Object object) {
    return record.equals(object);
  }

  @Override
  public int hashCode() {
    return record.hashCode();
  }

  @Override
  public String toString() {
    return record.toString();
  }

  /**
   * Method size returns the number of values in this Tuple instance.
   *
   * @return int
   */
  @Override
  public int size() {
    return schema.getFields().size();
  }

  private final void verifyModifiable()
  {
    if( isUnmodifiable )
      throw new UnsupportedOperationException( "this tuple is unmodifiable" );
  }



  public GenericData.Record getRecord() {
    return record;
  }
}
