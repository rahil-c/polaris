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
package org.apache.polaris.service.catalog.generic;

import static org.apache.polaris.service.catalog.conversion.xtable.XTableConvertorConfigurations.ENABLED_READ_TABLE_FORMATS_KEY;
import static org.apache.polaris.service.catalog.conversion.xtable.XTableConvertorConfigurations.TARGET_FORMAT_METADATA_PATH_KEY;

import jakarta.ws.rs.core.SecurityContext;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.conversion.xtable.RemoteXTableConvertor;
import org.apache.polaris.service.catalog.conversion.xtable.TableFormat;
import org.apache.polaris.service.catalog.conversion.xtable.XTableConversionUtils;
import org.apache.polaris.service.catalog.conversion.xtable.models.ConvertTableResponse;
import org.apache.polaris.service.catalog.conversion.xtable.models.ConvertedTable;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogHandler;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.service.types.ListGenericTablesResponse;
import org.apache.polaris.service.types.LoadGenericTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericTableCatalogHandler extends CatalogHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenericTableCatalogHandler.class);

  private PolarisMetaStoreManager metaStoreManager;

  private GenericTableCatalog genericTableCatalog;

  public GenericTableCatalogHandler(
      CallContext callContext,
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      SecurityContext securityContext,
      String catalogName,
      PolarisAuthorizer authorizer) {
    super(callContext, entityManager, securityContext, catalogName, authorizer);
    this.metaStoreManager = metaStoreManager;
  }

  @Override
  protected void initializeCatalog() {
    this.genericTableCatalog =
        new PolarisGenericTableCatalog(metaStoreManager, callContext, this.resolutionManifest);
    this.genericTableCatalog.initialize(catalogName, Map.of());
    initializeConversionServiceIfEnabled();
  }

  public ListGenericTablesResponse listGenericTables(Namespace parent) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    authorizeBasicNamespaceOperationOrThrow(op, parent);

    return ListGenericTablesResponse.builder()
        .setIdentifiers(new LinkedHashSet<>(genericTableCatalog.listGenericTables(parent)))
        .build();
  }

  public LoadGenericTableResponse createGenericTable(
      TableIdentifier identifier, String format, String doc, Map<String, String> properties) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_TABLE_DIRECT;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);

    GenericTableEntity createdEntity =
        this.genericTableCatalog.createGenericTable(identifier, format, doc, properties);
    GenericTable createdTable =
        GenericTable.builder()
            .setName(createdEntity.getName())
            .setFormat(createdEntity.getFormat())
            .setDoc(createdEntity.getDoc())
            .setProperties(createdEntity.getPropertiesAsMap())
            .build();
    return LoadGenericTableResponse.builder().setTable(createdTable).build();
  }

  public boolean dropGenericTable(TableIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE;
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);

    return this.genericTableCatalog.dropGenericTable(identifier);
  }

  public LoadGenericTableResponse loadGenericTable(TableIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_TABLE;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.GENERIC_TABLE, identifier);

    Optional<LoadGenericTableResponse> maybeConversionResponse =
            loadTableViaIcebergTableIfApplicable(identifier);
    if (maybeConversionResponse.isPresent()) {
      return maybeConversionResponse.get();
    }
    GenericTableEntity loadedEntity = this.genericTableCatalog.loadGenericTable(identifier);
    // TODO adding this for now,
    //  as there is a bug with polaris spark client not persisting table properties
    // once bug is fixed we can get proper value for this key (i.e "ICEBERG", "HUDI", "DELTA")
    Map<String, String> modifiedProps = loadedEntity.getPropertiesAsMap();
    modifiedProps.put(ENABLED_READ_TABLE_FORMATS_KEY, TableFormat.ICEBERG.name());
    GenericTable loadedTable =
        GenericTable.builder()
            .setName(loadedEntity.getName())
            .setFormat(loadedEntity.getFormat())
            .setDoc(loadedEntity.getDoc())
            .setProperties(modifiedProps)
            .build();

    return LoadGenericTableResponse.builder().setTable(loadedTable).build();
  }

  private Optional<LoadGenericTableResponse> loadTableViaIcebergTableIfApplicable(TableIdentifier tableIdentifier) {
    if (!XTableConversionUtils.requiresConversion(callContext)) {
      return Optional.empty();
    }
    PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(tableIdentifier);
    IcebergTableLikeEntity tableLikeEntity = IcebergTableLikeEntity.of(target.getRawLeafEntity());
    if (tableLikeEntity == null) {
      return Optional.empty();
    } else if (tableLikeEntity.getSubType() == PolarisEntitySubType.ICEBERG_TABLE) {
      RemoteXTableConvertor remoteXTableConvertor = RemoteXTableConvertor.getInstance();
      Optional<ConvertTableResponse> optionalConvertTableResponse = Optional.ofNullable(remoteXTableConvertor.execute(tableLikeEntity));
      if (optionalConvertTableResponse.isEmpty()) {
        return Optional.empty();
      } else {
        ConvertedTable convertedTable = optionalConvertTableResponse.get().getConvertedTables().get(0);
        if (convertedTable.getTargetMetadataPath() == null) {
          LOGGER.debug("Received a null target-metadata-path after table conversion");
          return Optional.empty();
        } else {
          Map<String, String> genericTableProps = new HashMap<>();
          genericTableProps.put(TARGET_FORMAT_METADATA_PATH_KEY, convertedTable.getTargetMetadataPath());
          GenericTable genericTable = GenericTable.builder()
                  .setFormat(convertedTable.getTargetFormat())
                  .setName(tableIdentifier.name())
                  .setProperties(genericTableProps)
                  .build();
          return Optional.of(LoadGenericTableResponse.builder().setTable(genericTable).build());
        }
      }
    }
    return Optional.empty();
  }
}
