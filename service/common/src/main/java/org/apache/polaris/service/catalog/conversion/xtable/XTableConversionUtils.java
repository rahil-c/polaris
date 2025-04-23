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

package org.apache.polaris.service.catalog.conversion.xtable;

import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;

import java.util.Map;

import static org.apache.polaris.service.catalog.conversion.xtable.XTableConvertorConfigurations.ENABLED_READ_TABLE_FORMATS_KEY;
import static org.apache.polaris.service.catalog.conversion.xtable.XTableConvertorConfigurations.SOURCE_METADATA_PATH_KEY;

public class XTableConversionUtils {

    private XTableConversionUtils() {}

    public static boolean requiresConversion(CallContext context, Map<String, String> tableProperties) {
        boolean conversionServiceEnabled =
                context.getPolarisCallContext()
                        .getConfigurationStore()
                        .getConfiguration(
                                context.getPolarisCallContext(), FeatureConfiguration.ENABLE_XTABLE_REST_SERVICE);
        return conversionServiceEnabled
                && tableProperties.containsKey(ENABLED_READ_TABLE_FORMATS_KEY)
                && tableProperties.containsKey(SOURCE_METADATA_PATH_KEY);
    }

    public static String getHostUrl(CallContext context) {
        return context
                .getPolarisCallContext()
                .getConfigurationStore()
                .getConfiguration(
                        context.getPolarisCallContext(), FeatureConfiguration.XTABLE_REST_SERVICE_HOST_URL);
    }
}
