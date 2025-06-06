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

import jakarta.ws.rs.core.SecurityContext;
import java.util.LinkedHashSet;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.conversion.xtable.RemoteXTableConvertor;
import org.apache.polaris.service.catalog.conversion.xtable.XTableConversionUtils;
import org.apache.polaris.service.catalog.conversion.xtable.models.ConvertTableResponse;
import org.apache.polaris.service.catalog.conversion.xtable.models.ConvertedTable;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.service.types.ListGenericTablesResponse;
import org.apache.polaris.service.types.LoadGenericTableResponse;

public class GenericTableCatalogHandler extends CatalogHandler {

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

  public LoadGenericTableResponse loadGenericTable(
      TableIdentifier identifier,
      String enabledReadTableFormat) { // additional format param or some query params
    // For now assume it will be iceberg

    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_TABLE;
    authorizeBasicTableLikeOperationOrThrow(op, PolarisEntitySubType.GENERIC_TABLE, identifier);
    GenericTableEntity loadedEntity = this.genericTableCatalog.loadGenericTable(identifier);

    /*
    we now do conversion, based on the query param
    we get back table response with some target-metadata-path
     */

    // TODO adding this for now,
    //  as there is a bug with polaris spark client not persisting table properties
    // once bug is fixed we can get proper value for this key (i.e "ICEBERG", "HUDI", "DELTA")

    // now at this point when constructing the GenericTable response we have some options,
    /*
    option 1 is we put location instead of the generic table location as not this metadata path
    option 2 is keep another field called target metadata path
     */

    GenericTable loadedTable =
        GenericTable.builder()
            .setName(loadedEntity.getName())
            .setFormat(loadedEntity.getFormat())
            .setDoc(loadedEntity.getDoc())
            .setProperties(loadedEntity.getPropertiesAsMap())
            .build();
    // if query param is present and its format is different
    if (enabledReadTableFormat != null) {
      GenericTable convertedGenericTable = convertGenericTableIfRequired(loadedEntity, loadedTable, enabledReadTableFormat);
      return LoadGenericTableResponse.builder().setTable(convertedGenericTable).build();
    }

    return LoadGenericTableResponse.builder().setTable(loadedTable).build();
  }

  private GenericTable convertGenericTableIfRequired(
      GenericTableEntity entity, GenericTable originalTable, String enabledReadTableFormat) {
    if (XTableConversionUtils.requiresConversion(callContext)) {
      ConvertTableResponse response =
          RemoteXTableConvertor.getInstance().execute(entity, enabledReadTableFormat);
      ConvertedTable convertedTable = response.getConvertedTables().get(0);
      // lets try override location idea first, and then try adding another field just to see what
      // happens.
      Map<String, String> genericTableProps = originalTable.getProperties();
      genericTableProps.put(
              "provider",
              convertedTable
                      .getTargetFormat());
      genericTableProps.put(
          "location",
          convertedTable
              .getTargetMetadataPath()); // lets try metadata path and see what happens if not we

      GenericTable genericConvertedTable =
              GenericTable.builder()
                      .setName(entity.getName())
                      .setFormat(convertedTable.getTargetFormat())
                      .setDoc(entity.getDoc())
                      .setProperties(genericTableProps)
                      .build();

      return genericConvertedTable;
      // can try base path next
    }
    return originalTable;
  }
}
