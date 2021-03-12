/*
 * Copyright [2021 - 2021] Confluent Inc.
 */

package io.confluent.connect.hdfs.wal;

import java.io.IOException;

public class CorruptWalFileException extends IOException {
  private static final long serialVersionUID = 1L;

  public CorruptWalFileException(String s) {
    super(s);
  }
}
