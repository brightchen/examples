package com.example.kafka.exactlyonce;

import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.Pair;
import com.datatorrent.contrib.kafka.AbstractTupleUniqueExactlyOnceKafkaOutputOperator;
import com.datatorrent.contrib.kafka.KafkaMetadataUtil;
import com.google.common.collect.Sets;

import kafka.serializer.StringDecoder;

/**
 * This operator assume the type of input is Long
 *
 */
public class TupleUniqueExactlyOnceKafkaOutputOperator
    extends AbstractTupleUniqueExactlyOnceKafkaOutputOperator<Long, String, String>
{
  private static final Logger logger = LoggerFactory.getLogger(TupleUniqueExactlyOnceKafkaOutputOperator.class);
  
  protected transient StringDecoder decoder = new StringDecoder(null);
  protected int operatorPartitions = 2;
  
  // properties
  protected String metadataBrokerList;
  
  @Override
  public void setup(OperatorContext context)
  {
    setConfigProperties(getKafkaProperties());
    super.setup(context);
  }
  
  @Override
  protected void processTuple(Long tuple)
  {
    if (tuple <= 0) {
      //to test the recovery
      throw new RuntimeException("Invalid tuple.");
    }

    super.processTuple(tuple);
  }

  @Override
  protected Pair<String, String> tupleToKeyValue(Long tuple)
  {
    return new Pair<>(String.valueOf(tuple % operatorPartitions), String.valueOf(tuple));
  }
  
  @Override
  protected Set<String> getBrokerSet()
  {
    if (brokerSet == null) {
      String brokerList = metadataBrokerList;
      if(brokerList == null || brokerList.isEmpty()) {
        brokerList = (String)getConfigProperties().get(KafkaMetadataUtil.PRODUCER_PROP_BROKERLIST);
      } else {
        getConfigProperties().setProperty(KafkaMetadataUtil.PRODUCER_PROP_BROKERLIST, brokerList);
      }
      brokerSet = Sets.newHashSet(brokerList.split(","));
    }
    return brokerSet;
  }
  
  @Override
  protected <T> boolean equals(byte[] bytes, T value)
  {
    if (bytes == null && value == null) {
      return true;
    }
    if (bytes == null ^ value == null) {
      return false;
    }
    return value.equals(decoder.fromBytes(bytes));
  }

  public Properties getKafkaProperties()
  {
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", metadataBrokerList);
    props.setProperty("producer.type", "async");
    props.setProperty("queue.buffering.max.ms", "100");
    props.setProperty("queue.buffering.max.messages", "5");
    props.setProperty("batch.num.messages", "5");
    return props;
  }
  
  public int getOperatorPartitions()
  {
    return operatorPartitions;
  }

  public void setOperatorPartitions(int operatorPartitions)
  {
    this.operatorPartitions = operatorPartitions;
  }

  public String getMetadataBrokerList()
  {
    return metadataBrokerList;
  }

  
  public void setMetadataBrokerList(String metadataBrokerList)
  {
    this.metadataBrokerList = metadataBrokerList;
  }
}
