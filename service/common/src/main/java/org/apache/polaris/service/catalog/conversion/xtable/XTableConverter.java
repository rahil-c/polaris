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

import static org.apache.polaris.service.catalog.conversion.xtable.XTableConvertorConfigurations.ENABLED_READ_TABLE_FORMATS_KEY;
import static org.apache.polaris.service.catalog.conversion.xtable.XTableConvertorConfigurations.SOURCE_METADATA_PATH_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import com.google.common.annotations.VisibleForTesting;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.entity.table.TableLikeEntity;

public final class XTableConverter {
    private static final int SUCCESS_STATUS_CODE = 200;
    private static final String RUN_SYNC_ENDPOINT = "/v1/conversion/sync";

    private final String hostUrl;
    private final HttpClient client;
    private final ObjectMapper objectMapper;

    public XTableConverter(String hostUrl) {
        this.hostUrl = hostUrl;
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        this.objectMapper = new ObjectMapper();

    }

    @VisibleForTesting
    XTableConverter(String hostUrl, HttpClient client, ObjectMapper objectMapper) {
        this.hostUrl = hostUrl;
        this.client = client;
        this.objectMapper = objectMapper;
    }

    public RunSyncResponse execute(TableLikeEntity tableEntity) {
        String sourceFormat;
        String sourceMetadataPath;
        String targetFormat;

        switch (tableEntity.getSubType()) {
            case GENERIC_TABLE -> {
                GenericTableEntity generic = (GenericTableEntity) tableEntity;
                sourceFormat = checkIfSupportedFormat(generic.getFormat());
                sourceMetadataPath = generic.getPropertiesAsMap().get(SOURCE_METADATA_PATH_KEY);
                targetFormat = checkIfSupportedFormat(
                        generic.getPropertiesAsMap().get(ENABLED_READ_TABLE_FORMATS_KEY));
            }
            case ICEBERG_TABLE -> {
                IcebergTableLikeEntity iceberg = (IcebergTableLikeEntity) tableEntity;
                sourceFormat = TableFormat.ICEBERG.name();
                sourceMetadataPath = iceberg.getMetadataLocation();
                targetFormat = iceberg.getPropertiesAsMap().get(ENABLED_READ_TABLE_FORMATS_KEY);
            }
            default -> throw new IllegalArgumentException(
                    "Unsupported TableEntity type: " + tableEntity.getSubType());
        }

        return executeRunSyncRequest(sourceFormat, sourceMetadataPath, targetFormat);
    }

    private RunSyncResponse executeRunSyncRequest(
            String sourceFormat,
            String sourceMetadataPath,
            String targetFormat) {

        RunSyncRequest request = new RunSyncRequest(sourceFormat, sourceMetadataPath, targetFormat);
        try {
            String requestBody = objectMapper.writeValueAsString(request);
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(hostUrl + RUN_SYNC_ENDPOINT))
                    .header("Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();

            HttpResponse<String> response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != SUCCESS_STATUS_CODE) {
                throw new IllegalStateException("Conversion failed: " + response.body());
            }
            return objectMapper.readValue(response.body(), RunSyncResponse.class);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static String checkIfSupportedFormat(String format) {
        TableFormat tableFormat = TableFormat.fromName(format);
        return tableFormat.name();
    }
}