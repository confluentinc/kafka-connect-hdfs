package io.confluent.connect.hdfs.jdbc;

import org.mockito.ArgumentMatcher;
import org.mockito.internal.matchers.CapturesArguments;
import org.mockito.internal.matchers.VarargMatcher;

import java.sql.Blob;
import java.sql.Clob;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.internal.exceptions.Reporter.noArgumentValueWasCaptured;

/**
 * Used for mapping an argument/value before storing it in the Captor.
 * Useful for transient/Closeable values, like results from SQL queries.
 */
public class MappedCaptor<T, U> implements ArgumentMatcher<T>, CapturesArguments, VarargMatcher {
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Lock readLock = lock.readLock();
  private final Lock writeLock = lock.writeLock();
  private final MappedFunction<T, U> mappedFunction;
  private final List<U> arguments = new ArrayList<>();

  @FunctionalInterface
  public interface MappedFunction<T, R> {
    R apply(T var1) throws Exception;
  }

  public static MappedCaptor<Blob, byte[]> blobCaptor() {
    return new MappedCaptor<>(blob -> (blob != null) ? blob.getBytes(1L, (int) blob.length()) : null);
  }

  public static MappedCaptor<Clob, String> clobCaptor() {
    return new MappedCaptor<>(clob -> (clob != null) ? clob.getSubString(1L, (int) clob.length()) : null);
  }

  public MappedCaptor(MappedFunction<T, U> mappedFunction) {
    this.mappedFunction = mappedFunction;
  }

  public boolean matches(Object argument) {
    return true;
  }

  public String toString() {
    return arguments
        .stream()
        .map(argument -> {
          if (argument instanceof String) {
            return (String) argument;
          }
          if (argument instanceof byte[]) {
            return "bytes[" + ((byte[]) argument).length + "]";
          }
          return "<argument>";
        })
        .map(Objects::toString)
        .collect(Collectors.joining(";", "SafeCaptor{", "}"));
  }

  public U getOnlyValue() {
    readLock.lock();
    try {
      assertEquals(1, arguments.size());

      return arguments.get(arguments.size() - 1);
    } finally {
      readLock.unlock();
    }
  }

  public U getLastValue() {
    readLock.lock();
    try {
      if (arguments.isEmpty()) {
        throw noArgumentValueWasCaptured();
      }

      return arguments.get(arguments.size() - 1);
    } finally {
      readLock.unlock();
    }
  }

  public List<U> getAllValues() {
    readLock.lock();
    try {
      return new ArrayList<>(arguments);
    } finally {
      readLock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  public void captureFrom(Object argument) {
    writeLock.lock();
    try {
      this.arguments.add(mappedFunction.apply((T) argument));
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      writeLock.unlock();
    }
  }
}
