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

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.catalog.api.PolarisCatalogGenericTableApiService;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.ListGenericTablesResponse;
import org.apache.polaris.service.types.LoadGenericTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
public class GenericTableCatalogAdapter implements PolarisCatalogGenericTableApiService {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenericTableCatalogAdapter.class);

  private final CallContext callContext;
  private final PolarisEntityManager entityManager;
  private final PolarisMetaStoreManager metaStoreManager;
  private final PolarisAuthorizer polarisAuthorizer;

  @Inject
  public GenericTableCatalogAdapter(
      CallContext callContext,
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      PolarisAuthorizer polarisAuthorizer) {
    this.callContext = callContext;
    this.entityManager = entityManager;
    this.metaStoreManager = metaStoreManager;
    this.polarisAuthorizer = polarisAuthorizer;
  }

  private Namespace rawStringToNamespace(String namespace) {
    String[] components = namespace.split("\u001F");
    return Namespace.of(components);
  }

  private GenericTableCatalogHandler newHandlerWrapper(
      SecurityContext securityContext, String catalogName) {
    AuthenticatedPolarisPrincipal authenticatedPrincipal =
        (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
    if (authenticatedPrincipal == null) {
      throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
    }

    return new GenericTableCatalogHandler(
        callContext,
        entityManager,
        metaStoreManager,
        securityContext,
        catalogName,
        polarisAuthorizer);
  }

  @Override
  public Response createGenericTable(
      String prefix,
      String namespace,
      CreateGenericTableRequest createGenericTableRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    GenericTableCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    LoadGenericTableResponse response =
        handler.createGenericTable(
            TableIdentifier.of(
                rawStringToNamespace(namespace), createGenericTableRequest.getName()),
            createGenericTableRequest.getFormat(),
            createGenericTableRequest.getDoc(),
            createGenericTableRequest.getProperties());

    return Response.ok(response).build();
  }

  @Override
  public Response dropGenericTable(
      String prefix,
      String namespace,
      String genericTable,
      RealmContext realmContext,
      SecurityContext securityContext) {
    GenericTableCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    boolean success =
        handler.dropGenericTable(TableIdentifier.of(rawStringToNamespace(namespace), genericTable));

    return Response.status(Response.Status.OK).build();
  }

  @Override
  public Response listGenericTables(
      String prefix,
      String namespace,
      String pageToken,
      Integer pageSize,
      RealmContext realmContext,
      SecurityContext securityContext) {
    GenericTableCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    ListGenericTablesResponse response = handler.listGenericTables(rawStringToNamespace(namespace));
    return Response.ok(response).build();
  }

  @Override
  public Response loadGenericTable(
      String prefix,
      String namespace,
      String genericTable,
      RealmContext realmContext,
      SecurityContext securityContext) {
    GenericTableCatalogHandler handler = newHandlerWrapper(securityContext, prefix);
    LoadGenericTableResponse response =
        handler.loadGenericTable(TableIdentifier.of(rawStringToNamespace(namespace), genericTable));
    return Response.ok(response).build();
  }
}
