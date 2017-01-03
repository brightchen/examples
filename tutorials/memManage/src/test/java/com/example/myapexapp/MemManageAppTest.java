/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

/**
 * Test the DAG declaration in local mode.
 */
public class MemManageAppTest extends MemManageApp 
{
  private static final Logger logger = LoggerFactory.getLogger(MemManageAppTest.class);
  public static final String stramPath = "com.datatorrent.stram.StramLocalCluster";

  @Before
  public void before()
  {
    FileUtil.fullyDelete(new File(stramPath));
  }
  
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
    lc.run(600000);

    lc.shutdown();

  }
}
