/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.conversion;

import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.entity.table.TableLikeEntity;

/** Implementations are used to convert from one table format to another. */
public interface TableConverter {

  /**
   * Initialize a converter given a custom name and a map of converter properties.
   *
   * @param name a custom name for the converter
   * @param properties converter properties
   */
  void initialize(String name, Map<String, String> properties);

  /**
   * Returns a converted version of the given {@link TableLikeEntity}, or Optional.empty() if the
   * table can't be converted for some reason. The converted table should be at most
   * `requestedFreshnessSeconds` behind the source table.
   *
   * @param table the table to convert
   * @param storageCredentials
   * @param requestedFreshnessSeconds the maximum requested lag between the source table and the
   *     converted table
   */
  Optional<TableLikeEntity> convert(
      TableLikeEntity table, Map<String, String> storageCredentials, int requestedFreshnessSeconds);

  /**
   * Returns a converted version of the given {@link TableLikeEntity}, or Optional.empty() if the
   * table can't be converted for some reason.
   *
   * @param table the table to convert
   * @param storageCredentials
   */
  default Optional<TableLikeEntity> convert(
      TableLikeEntity table, Map<String, String> storageCredentials) {
    return convert(table, storageCredentials, 0);
  }
}
