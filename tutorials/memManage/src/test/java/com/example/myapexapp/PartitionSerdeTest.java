package com.example.myapexapp;

import org.apache.apex.engine.serde.GenericSerde;
import org.apache.apex.engine.serde.PartitionSerde;
import org.apache.apex.engine.serde.SerializationBuffer;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.netlet.util.Slice;

public class PartitionSerdeTest
{
  @Test
  public void test()
  {
    String payload = "1234567890";
    SerializationBuffer serializationBuffer = SerializationBuffer.READ_BUFFER;
    PartitionSerde partitionSerde = PartitionSerde.DEFAULT;
    Slice slice1 = partitionSerde.serialize(payload.hashCode(), payload, serializationBuffer);
    byte[] array1 = new byte[slice1.length];
    System.arraycopy(slice1.buffer, slice1.offset, array1, 0, slice1.length);
    
    GenericSerde.DEFAULT.serialize(payload, serializationBuffer);
    Slice slice2 = serializationBuffer.toSlice();
    byte[] array2 = PayloadTuple.getSerializedTuple(payload.hashCode(), slice2);

    Assert.assertArrayEquals(array1, array2);
  }
}
