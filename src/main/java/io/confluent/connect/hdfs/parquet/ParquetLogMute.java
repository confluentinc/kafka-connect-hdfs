package io.confluent.connect.hdfs.parquet;

import java.util.logging.Handler;
import java.util.logging.Logger;

public class ParquetLogMute {
  public static void mute() {
    try {
      Class.forName("org.apache.parquet.Log");
    } catch (Throwable ignored) {
    }

    try {
      Class.forName("parquet.Log");
    } catch (Throwable ignored) {
    }

    for (final String pack : new String[]{"org.apache.parquet", "parquet"}) {
      try {
        final Logger logger = Logger.getLogger(pack);
        for (final Handler handler : logger.getHandlers()) {
          logger.removeHandler(handler);
        }
        logger.setUseParentHandlers(false);
      } catch (Throwable ignored) {
      }
    }
  }
}
