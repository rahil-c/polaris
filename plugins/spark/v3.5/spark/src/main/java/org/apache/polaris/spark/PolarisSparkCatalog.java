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
package org.apache.polaris.spark;

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.spark.utils.PolarisCatalogUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A spark TableCatalog Implementation interacts with Polaris specific APIs only. The APIs it
 * interacts with is generic table APIs, and all table operations performed in this class are
 * expected to be for non-iceberg tables.
 */
public class PolarisSparkCatalog implements TableCatalog {

  private PolarisCatalog polarisCatalog = null;
  private String catalogName = null;

  public PolarisSparkCatalog(PolarisCatalog polarisCatalog) {
    this.polarisCatalog = polarisCatalog;
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Table loadTable(Identifier identifier) throws NoSuchTableException {
    try {
      // i think format query param wont be able to work since its confined to the interface which
      // does not allow.
      // HOWEVER doing intial creation we would add some table prop there

      GenericTable genericTable;
      // need to put try catch aroudn this
      String enabledReadTableFormat =
          SparkSession.active().conf().get("enabledReadTableFormat", null);
      if (enabledReadTableFormat == null) {
        genericTable =
            this.polarisCatalog.loadGenericTable(
                Spark3Util.identifierToTableIdentifier(identifier));
      } else {
        genericTable =
            this.polarisCatalog.loadGenericTable(
                Spark3Util.identifierToTableIdentifier(identifier), enabledReadTableFormat);
      }

      // ok i have an idea i think this can work.

      /*
      so a delta generic table was created, with a delta polaris spark client.
      another spark session is start with iceberg polaris spark client.
      that client,

      ideally after its gone thru the entire flow there should be two ops
      a target metadata path, or location has been set to that path

      spark client format side now just needs to load that depeing on how that client was configured

       */
      return PolarisCatalogUtils.loadSparkTable(genericTable);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(identifier);
    }
  }

  @Override
  public Table createTable(
      Identifier identifier,
      StructType schema,
      Transform[] transforms,
      Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    try {
      String format = properties.get(PolarisCatalogUtils.TABLE_PROVIDER_KEY);
      GenericTable genericTable =
          this.polarisCatalog.createGenericTable(
              Spark3Util.identifierToTableIdentifier(identifier), format, null, properties);
      return PolarisCatalogUtils.loadSparkTable(genericTable);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(identifier);
    }
  }

  @Override
  public Table alterTable(Identifier identifier, TableChange... changes)
      throws NoSuchTableException {
    // alterTable currently is not supported for generic tables
    throw new UnsupportedOperationException("alterTable operation is not supported");
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    // purgeTable for generic table will only do a drop without purge
    return dropTable(ident);
  }

  @Override
  public boolean dropTable(Identifier identifier) {
    return this.polarisCatalog.dropGenericTable(Spark3Util.identifierToTableIdentifier(identifier));
  }

  @Override
  public void renameTable(Identifier from, Identifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("renameTable operation is not supported");
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    try {
      return this.polarisCatalog.listGenericTables(Namespace.of(namespace)).stream()
          .map(ident -> Identifier.of(ident.namespace().levels(), ident.name()))
          .toArray(Identifier[]::new);
    } catch (UnsupportedOperationException ex) {
      return new Identifier[0];
    }
  }
}
