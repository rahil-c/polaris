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
package org.apache.polaris.core.policy.content.maintenance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.policy.content.PolicyContentUtil;
import org.apache.polaris.core.policy.validator.InvalidPolicyException;

public class TableConversionPolicyContent extends BaseMaintenancePolicyContent {
  private static final String DEFAULT_POLICY_SCHEMA_VERSION = "2025-02-03";
  private static final Set<String> POLICY_SCHEMA_VERSIONS = Set.of(DEFAULT_POLICY_SCHEMA_VERSION);

  private static final Set<String> SUPPORTED_TABLE_FORMATS = Set.of("ICEBERG", "DELTA", "HUDI");
  private static final Set<String> SUPPORTED_CONVERSION_SERVICES =
      Set.of("xtable", "remote-xtable");

  @JsonProperty(value = "conversionService")
  private String conversionService;

  @JsonProperty(value = "targetTableFormats")
  private List<String> targetTableFormats;

  @JsonProperty(value = "configurations")
  private Map<String, String> configurations;

  @JsonCreator
  public TableConversionPolicyContent(
      @JsonProperty(value = "enable", required = true) boolean enable,
      @JsonProperty(value = "conversionService", required = true) String conversionService,
      @JsonProperty(value = "targetTableFormats", required = true) List<String> targetTableFormats,
      @JsonProperty(value = "configurations") Map<String, String> configurations) {
    super(enable);
    this.conversionService = conversionService;
    this.targetTableFormats = targetTableFormats;
    this.configurations = configurations;
  }

  public String getConversionService() {
    return conversionService;
  }

  public void setConversionService(String conversionService) {
    this.conversionService = conversionService;
  }

  public List<String> getTargetTableFormats() {
    return targetTableFormats;
  }

  public void setTargetTableFormats(List<String> targetTableFormats) {
    this.targetTableFormats = targetTableFormats;
  }

  public Map<String, String> getConfigurations() {
    return configurations;
  }

  public void setConfigurations(Map<String, String> configurations) {
    this.configurations = configurations;
  }

  public static TableConversionPolicyContent fromString(String content) {
    if (Strings.isNullOrEmpty(content)) {
      throw new InvalidPolicyException("Policy is empty");
    }

    TableConversionPolicyContent policy;
    try {
      policy = PolicyContentUtil.MAPPER.readValue(content, TableConversionPolicyContent.class);
    } catch (Exception e) {
      throw new InvalidPolicyException(e);
    }

    validateVersion(content, policy, DEFAULT_POLICY_SCHEMA_VERSION, POLICY_SCHEMA_VERSIONS);

    String conversionService = policy.getConversionService();
    if (Strings.isNullOrEmpty(conversionService)) {
      throw new InvalidPolicyException("conversionService must be specified");
    }

    if (!SUPPORTED_CONVERSION_SERVICES.contains(conversionService)) {
      throw new InvalidPolicyException(
          "Unsupported conversionService: "
              + conversionService
              + ". Supported services: "
              + SUPPORTED_CONVERSION_SERVICES);
    }

    List<String> targetFormats = policy.getTargetTableFormats();
    if (targetFormats == null || targetFormats.isEmpty()) {
      throw new InvalidPolicyException("targetTableFormats must be specified and non-empty");
    }

    for (String format : targetFormats) {
      if (Strings.isNullOrEmpty(format)) {
        throw new InvalidPolicyException("targetTableFormats cannot contain null or empty values");
      }
      String upperFormat = format.toUpperCase(Locale.ROOT);
      if (!SUPPORTED_TABLE_FORMATS.contains(upperFormat)) {
        throw new InvalidPolicyException(
            "Unsupported table format: "
                + format
                + ". Supported formats: "
                + SUPPORTED_TABLE_FORMATS);
      }
    }

    return policy;
  }
}
