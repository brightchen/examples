package com.example.myapexapp;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;

@ApplicationAnnotation(name="MemManage")
public class MemManageApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Generator generator = new Generator();
    dag.addOperator("generator", generator);

    OutputOperator output = new OutputOperator();
    dag.addOperator("output", output);

    dag.addStream("stream", generator.output, output.data).setLocality(DAG.Locality.NODE_LOCAL);
  }

  public static class Generator implements InputOperator
  {
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
    protected char[] chars;

    private int numOfValues = 100000;
    private transient String[] values = new String[numOfValues];
    private Random random = new Random();
    private int valueLen = 1000;

    private void initValues()
    {
      //init chars
      chars = new char[26*2 + 10];

      int i = 0;
      for(; i<26; ++i) {
        chars[i] = (char)('A' + i);
      }
      for(; i<52; ++i) {
        chars[i] = (char)('a' + i - 26);
      }
      for(; i<chars.length; ++i) {
        chars[i] = (char)('0' + i - 52);
      }

      char[] chars1 = new char[valueLen];
      for(i=0; i<values.length; ++i){
        for(int j=0; j<valueLen; ++j) {
          chars1[j] = chars[random.nextInt(chars.length)];
        }
        values[i] = new String(chars1);
      }
    }

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

    private long beginTime;
    @Override
    public void setup(OperatorContext context)
    {
      initValues();
      beginTime = System.currentTimeMillis();
    }

    @Override
    public void teardown()
    {
    }

    private long emittedCount = 0;
    private int emitBreak = 1000;
    private int valueIndex = 0;

    @Override
    public void emitTuples()
    {
      for (; valueIndex < values.length && emittedCount < emitBreak; ++valueIndex, ++emittedCount) {
        output.emit(values[valueIndex]);
      }
      if (emittedCount == emitBreak) {
        emittedCount = 0;
        sleepSlient(1);
      }
      if (valueIndex == values.length) {
        valueIndex = 0;
      }
    }

    public void sleepSlient(int time)
    {
      try {
        Thread.sleep(time);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }


  public static class OutputOperator<T> extends BaseOperator
  {
    private static final Logger logger = LoggerFactory.getLogger(OutputOperator.class);

    private long totalCount = 0;
    private long count = 0;
    private long totalBeginTime;
    private long beginTime;

    public final transient DefaultInputPort<T> data = new DefaultInputPort<T>()
    {
      @Override
      public void process(T tuple)
      {
        processTuple(tuple);
      }
    };

    public void processTuple(T tuple)
    {
      ++count;
    }

    @Override
    public void setup(OperatorContext context)
    {
      totalBeginTime = System.currentTimeMillis();
      beginTime = totalBeginTime;
    }

    @Override
    public void endWindow()
    {
      long now = System.currentTimeMillis();
      if(now - beginTime >= 3000) {
        totalCount += count;
        logger.info("total: count: {}; average: {}", totalCount, totalCount * 1000 / (now - totalBeginTime));
        logger.info("period: count: {}; average: {}", count, count * 1000 / (now - beginTime));
        beginTime = now;
        count = 0;
      }
    }
  }
}
