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
package org.apache.polaris.extension.conversion.xtable;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.polaris.service.conversion.TableConverter;
import org.apache.polaris.service.conversion.TableFormat;
import org.apache.polaris.service.types.GenericTable;
import org.apache.xtable.conversion.ConversionController;

@ApplicationScoped
@Identifier("xtable")
public class XTableConverter implements TableConverter {

  private final ConversionController conversionController;

  public XTableConverter() {
    this(new Configuration());
  }

  public XTableConverter(Configuration configuration) {
    conversionController = new ConversionController(configuration);
  }

  // TODO convert locally
  @Override
  public Optional<GenericTable> convert(
      GenericTable table,
      TableFormat targetFormat,
      Map<String, String> storageCredentials,
      int requestedFreshnessSeconds) {
    // TODO fix pseudocode:
    //    SourceTable sourceTable =
    //        new SourceTable(
    //            table.getName(),
    //            table.getFormat(),
    //            table.getBaseLocation(),
    //            /* dataPath= */ null,
    //            /* namespace= */ null,
    //            /* catalogConfig= */ null,
    //            new Properties());
    //
    //    TargetTable targetTable =
    //        new TargetTable(
    //            table.getName(),
    //            targetFormat.toString(),
    //            table.getBaseLocation(),
    //            /* dataPath= */ null,
    //            /* namespace= */ null,
    //            /* catalogConfig= */ null,
    //            new Properties());
    //
    //    ConversionConfig conversionConfig =
    //        ConversionConfig.builder()
    //            .sourceTable(sourceTable)
    //            .targetTables(List.of(targetTable))
    //            .syncMode(SyncMode.FULL)
    //            .build();

    //    conversionController.sync(conversionConfig, /* conversionSourceProvider= */ null);
    return Optional.empty();
  }
}
