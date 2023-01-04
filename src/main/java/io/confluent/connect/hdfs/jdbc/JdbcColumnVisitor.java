/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs.jdbc;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;

public interface JdbcColumnVisitor {
  //void visit(String columnName, Array value);

  //void visit(String columnName, BigDecimal value);

  void visit(String columnName, Blob value) throws SQLException;

  //void visit(String columnName, Boolean value);

  //void visit(String columnName, Byte value);

  void visit(String columnName, Clob value) throws SQLException;

  //void visit(String columnName, Date value);

  //void visit(String columnName, Double value);

  //void visit(String columnName, Float value);

  //void visit(String columnName, Integer value);

  //void visit(String columnName, Long value);

  //void visit(String columnName, Ref value);

  //void visit(String columnName, Short value);

  void visit(String columnName, SQLXML value) throws SQLException;

  void visit(String columnName, String value);

  //void visit(String columnName, Struct value);

  //void visit(String columnName, Time value);

  //void visit(String columnName, Timestamp value);
}
