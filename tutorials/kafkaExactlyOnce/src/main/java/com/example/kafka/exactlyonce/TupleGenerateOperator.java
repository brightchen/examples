package com.example.kafka.exactlyonce;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * generate tuple only.
 *
 */
public class TupleGenerateOperator extends BaseOperator implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(TupleGenerateOperator.class);
      
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Long> outputPort = new DefaultOutputPort<>();
  
  public static final long SCALE = 10000000L;
  protected int batchSize = 100;
  protected long value;

  protected transient long count = 0;
  protected transient Random radom = new Random(10);
  protected transient long breakCount = 0;
  protected transient boolean crash = false;

  @Override
  public void beginWindow(long windowId)
  {
    getBreakCount();
  }

  @Override
  public void emitTuples()
  {
    if (!outputPort.isConnected()) {
      return;
    }
    
    if (crash) {
      logger.info("Going to crash.");
      throw new RuntimeException("Want to crash.");
    }

    for (int i = 0; i < batchSize; ++i) {
      ++count;
      if (count == breakCount) {
        outputPort.emit(0L);
        logger.info("Emit 0.");
        breakCount = 0;
      } else {
        outputPort.emit(++value);
      }
    }
    waitMillis(1);
  }

  protected long getBreakCount()
  {
    if (breakCount <= 0) {
      int radomValue = radom.nextInt(10);
      if (radomValue == 9) {
        crash = true;
      }
      breakCount = (radomValue + 1) * SCALE;
      count = 0;
    }
    return breakCount;
  }

  protected void waitMillis(int millis)
  {
    try {
      Thread.sleep(millis);
    } catch (Exception e) {
      //ignore
    }
  }
}
