package com.example.kafka.exactlyonce;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

public class KafkaExactlyOnceAppTester extends KafkaExactlyOnceApp
{
  protected long runTime = 60000;

  @Test
  public void test() throws Exception
  {
    Configuration conf = new Configuration(false);
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    super.populateDAG(dag, conf);

    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.run(runTime);

    lc.shutdown();
  }
}
