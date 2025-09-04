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

import java.util.Locale;
import org.apache.polaris.service.conversion.xtable.RemoteXTableConverter;

/** Factory for creating TableConverter instances based on conversion service names. */
public class TableConverterFactory {

  /**
   * Creates a TableConverter instance based on the specified conversion service name.
   *
   * @param conversionService the name of the conversion service (e.g., "none", "xtable",
   *     "remote-xtable")
   * @return a TableConverter instance, or null if the service name is not recognized
   */
  public static TableConverter createConverter(String conversionService) {
    if (conversionService == null) {
      return null;
    }

    switch (conversionService.toLowerCase(Locale.ROOT)) {
      case "none":
        return new NoneTableConverter();
      case "xtable":
      case "remote-xtable":
        return new RemoteXTableConverter();
      default:
        throw new IllegalArgumentException(
            "Unsupported conversion service: "
                + conversionService
                + ". Supported services: none, xtable, remote-xtable");
    }
  }
}
