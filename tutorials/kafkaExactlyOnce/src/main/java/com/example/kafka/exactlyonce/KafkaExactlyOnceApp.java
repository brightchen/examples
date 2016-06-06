package com.example.kafka.exactlyonce;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;

@ApplicationAnnotation(name = "KafkaExactlyOnceApp")
public class KafkaExactlyOnceApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    TupleGenerateOperator generateOperator = new TupleGenerateOperator();
    dag.addOperator("generateOperator", generateOperator);
    
    TupleUniqueExactlyOnceKafkaOutputOperator kafkaOutputOperator = new TupleUniqueExactlyOnceKafkaOutputOperator();
    dag.addOperator("kafkaOutputOperator", kafkaOutputOperator);
    dag.addStream("generateStream", generateOperator.outputPort, kafkaOutputOperator.inputPort);
    
    dag.setAttribute(kafkaOutputOperator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<TupleGenerateOperator>(kafkaOutputOperator.operatorPartitions));
  }
}
