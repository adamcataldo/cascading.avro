package cascading.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public class AvroTupleSerialization extends Configured implements Serialization {

  private static final DecoderFactory FACTORY = DecoderFactory.get();

  public boolean accept(Class c) {
    return AvroTuple.class.isAssignableFrom(c);
  }

  @Override
  public Serializer getSerializer(Class c) {
    Schema inputSchema = AvroJob.getInputSchema(getConf());
    return new AvroTupleSerializer(new GenericDatumWriter<GenericData.Record>(inputSchema));
  }



  @Override
  public Deserializer getDeserializer(Class c) {
    Schema inputSchema = AvroJob.getInputSchema(getConf());
    return new AvroTupleDeserializer(new GenericDatumReader<GenericData.Record>(inputSchema));
  }


  private class AvroTupleSerializer implements Serializer<AvroTuple> {

    private OutputStream outputStream;
    private BinaryEncoder encoder;
    private final DatumWriter<GenericData.Record> writer;

    public AvroTupleSerializer(DatumWriter<GenericData.Record> writer) {
      this.writer = writer;
    }

    @Override
    public void open(OutputStream outputStream) throws IOException {
      this.outputStream = outputStream;
      this.encoder = new EncoderFactory().configureBlockSize(512)
          .binaryEncoder(outputStream, null);
    }

    @Override
    public void serialize(AvroTuple tuple) throws IOException {
      writer.write(tuple.getRecord(), encoder);
      encoder.flush();
    }

    @Override
    public void close() throws IOException {
      outputStream.close();
    }
  }

  private class AvroTupleDeserializer implements Deserializer<AvroTuple> {

    private final DatumReader<GenericData.Record> reader;
    private BinaryDecoder decoder;
    GenericData.Record record;
    InputStream inputStream;

    public AvroTupleDeserializer(DatumReader<GenericData.Record> reader) {
      this.reader = reader;
    }

    @Override
    public void open(InputStream inputStream) throws IOException {
      this.inputStream = inputStream;
      this.decoder = FACTORY.directBinaryDecoder(inputStream, decoder);
    }

    @Override
    public AvroTuple deserialize(AvroTuple objects) throws IOException {
      record = reader.read(record, decoder);
      return new AvroTuple(record);
    }

    @Override
    public void close() throws IOException {
      inputStream.close();
    }
  }
}
