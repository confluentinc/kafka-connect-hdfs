package io.confluent.connect.hdfs.jdbc;

import java.util.Optional;
import java.util.function.Predicate;

public class JdbcUtil {

  public static Optional<String> trimToNone(String value) {
    return Optional
        .ofNullable(value)
        .map(String::trim)
        .filter(((Predicate<String>) String::isEmpty).negate());
  }

  public static String trimToNull(String value) {
    return trimToNone(value)
        .orElse(null);
  }
}
