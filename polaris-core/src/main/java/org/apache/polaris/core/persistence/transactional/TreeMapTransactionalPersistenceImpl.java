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
package org.apache.polaris.core.persistence.transactional;

import com.google.common.base.Predicates;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.pagination.OffsetPageToken;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.pagination.PolarisPage;
import org.apache.polaris.core.persistence.pagination.ReadEverythingPageToken;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;

public class TreeMapTransactionalPersistenceImpl extends AbstractTransactionalPersistence {

  // the TreeMap store to use
  private final TreeMapMetaStore store;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final PrincipalSecretsGenerator secretsGenerator;

  public TreeMapTransactionalPersistenceImpl(
      @Nonnull TreeMapMetaStore store,
      @Nonnull PolarisStorageIntegrationProvider storageIntegrationProvider,
      @Nonnull PrincipalSecretsGenerator secretsGenerator) {

    // init store
    this.store = store;
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.secretsGenerator = secretsGenerator;
  }

  /** {@inheritDoc} */
  @Override
  public <T> T runInTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Supplier<T> transactionCode) {

    // run transaction on our underlying store
    return store.runInTransaction(callCtx, transactionCode);
  }

  /** {@inheritDoc} */
  @Override
  public void runActionInTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Runnable transactionCode) {

    // run transaction on our underlying store
    store.runActionInTransaction(callCtx, transactionCode);
  }

  /** {@inheritDoc} */
  @Override
  public <T> T runInReadTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Supplier<T> transactionCode) {
    // run transaction on our underlying store
    return store.runInReadTransaction(callCtx, transactionCode);
  }

  /** {@inheritDoc} */
  @Override
  public void runActionInReadTransaction(
      @Nonnull PolarisCallContext callCtx, @Nonnull Runnable transactionCode) {

    // run transaction on our underlying store
    store.runActionInReadTransaction(callCtx, transactionCode);
  }

  /**
   * @return new unique entity identifier
   */
  @Override
  public long generateNewIdInCurrentTxn(@Nonnull PolarisCallContext callCtx) {
    return this.store.getNextSequence();
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    // write it
    this.store.getSliceEntities().write(entity);
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      void persistStorageIntegrationIfNeededInCurrentTxn(
          @Nonnull PolarisCallContext callContext,
          @Nonnull PolarisBaseEntity entity,
          @Nullable PolarisStorageIntegration<T> storageIntegration) {
    // not implemented for in-memory store
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesActiveInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    // write it
    this.store.getSliceEntitiesActive().write(entity);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToEntitiesChangeTrackingInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    // write it
    this.store.getSliceEntitiesChangeTracking().write(entity);
  }

  /** {@inheritDoc} */
  @Override
  public void writeToGrantRecordsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    // write it
    this.store.getSliceGrantRecords().write(grantRec);
    this.store.getSliceGrantRecordsByGrantee().write(grantRec);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity) {

    // delete it
    this.store.getSliceEntities().delete(this.store.buildEntitiesKey(entity));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromEntitiesActiveInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity) {
    // delete it
    this.store.getSliceEntitiesActive().delete(this.store.buildEntitiesActiveKey(entity));
  }

  /**
   * {@inheritDoc}
   *
   * @param callCtx
   * @param entity entity record to delete
   */
  @Override
  public void deleteFromEntitiesChangeTrackingInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity) {
    // delete it
    this.store.getSliceEntitiesChangeTracking().delete(this.store.buildEntitiesKey(entity));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromGrantRecordsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {

    // delete it
    this.store.getSliceGrantRecords().delete(grantRec);
    this.store.getSliceGrantRecordsByGrantee().delete(grantRec);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllEntityGrantRecordsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore entity,
      @Nonnull List<PolarisGrantRecord> grantsOnGrantee,
      @Nonnull List<PolarisGrantRecord> grantsOnSecurable) {

    // build composite prefix key and delete grant records on the indexed side of each grant table
    String prefix = this.store.buildPrefixKeyComposite(entity.getCatalogId(), entity.getId());
    this.store.getSliceGrantRecords().deleteRange(prefix);
    this.store.getSliceGrantRecordsByGrantee().deleteRange(prefix);

    // also delete the other side. We need to delete these grants one at a time versus doing a
    // range delete
    grantsOnGrantee.forEach(gr -> this.store.getSliceGrantRecords().delete(gr));
    grantsOnSecurable.forEach(gr -> this.store.getSliceGrantRecordsByGrantee().delete(gr));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllInCurrentTxn(@Nonnull PolarisCallContext callCtx) {
    // clear all slices
    this.store.deleteAll();
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisBaseEntity lookupEntityInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
    return this.store.getSliceEntities().read(this.store.buildKeyComposite(catalogId, entityId));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisBaseEntity> lookupEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    // allocate return list
    return entityIds.stream()
        .map(
            id ->
                this.store
                    .getSliceEntities()
                    .read(this.store.buildKeyComposite(id.getCatalogId(), id.getId())))
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisChangeTrackingVersions> lookupEntityVersionsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    // allocate return list
    return entityIds.stream()
        .map(
            id ->
                this.store
                    .getSliceEntitiesChangeTracking()
                    .read(this.store.buildKeyComposite(id.getCatalogId(), id.getId())))
        .map(
            entity ->
                (entity != null)
                    ? new PolarisChangeTrackingVersions(
                        entity.getEntityVersion(), entity.getGrantRecordsVersion())
                    : null)
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public EntityNameLookupRecord lookupEntityActiveInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntitiesActiveKey entityActiveKey) {
    // lookup the active entity slice
    PolarisBaseEntity entity =
        this.store
            .getSliceEntitiesActive()
            .read(
                this.store.buildKeyComposite(
                    entityActiveKey.getCatalogId(),
                    entityActiveKey.getParentId(),
                    entityActiveKey.getTypeCode(),
                    entityActiveKey.getName()));

    // return record
    return (entity == null)
        ? null
        : new EntityNameLookupRecord(
            entity.getCatalogId(),
            entity.getId(),
            entity.getParentId(),
            entity.getName(),
            entity.getTypeCode(),
            entity.getSubTypeCode());
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<EntityNameLookupRecord> lookupEntityActiveBatchInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntitiesActiveKey> entityActiveKeys) {
    // now build a list to quickly verify that nothing has changed
    return entityActiveKeys.stream()
        .map(entityActiveKey -> this.lookupEntityActiveInCurrentTxn(callCtx, entityActiveKey))
        .collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PolarisPage<EntityNameLookupRecord> listEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PageToken pageToken) {
    return this.listEntitiesInCurrentTxn(
        callCtx, catalogId, parentId, entityType, Predicates.alwaysTrue(), pageToken);
  }

  @Override
  public @Nonnull PolarisPage<EntityNameLookupRecord> listEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull PageToken pageToken) {
    // full range scan under the parent for that type
    return this.listEntitiesInCurrentTxn(
        callCtx,
        catalogId,
        parentId,
        entityType,
        entityFilter,
        entity ->
            new EntityNameLookupRecord(
                entity.getCatalogId(),
                entity.getId(),
                entity.getParentId(),
                entity.getName(),
                entity.getTypeCode(),
                entity.getSubTypeCode()),
        pageToken);
  }

  @Override
  public @Nonnull <T> PolarisPage<T> listEntitiesInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer,
      @Nonnull PageToken pageToken) {
    if (!(pageToken instanceof ReadEverythingPageToken)
        && !(pageToken instanceof OffsetPageToken)) {
      throw new IllegalArgumentException("Unexpected pageToken: " + pageToken);
    }

    Stream<PolarisBaseEntity> partialResults =
        this.store
            .getSliceEntitiesActive()
            .readRange(
                this.store.buildPrefixKeyComposite(catalogId, parentId, entityType.getCode()))
            .stream()
            .map(
                nameRecord ->
                    this.lookupEntityInCurrentTxn(
                        callCtx, catalogId, nameRecord.getId(), entityType.getCode()))
            .filter(entityFilter);

    if (pageToken instanceof OffsetPageToken) {
      partialResults =
          partialResults
              .sorted(Comparator.comparingLong(PolarisEntityCore::getId))
              .skip(((OffsetPageToken) pageToken).offset)
              .limit(pageToken.pageSize);
    }

    List<T> entities = partialResults.map(transformer).collect(Collectors.toList());
    return pageToken.buildNextPage(entities);
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasChildrenInCurrentTxn(
      @Nonnull PolarisCallContext callContext,
      @Nullable PolarisEntityType entityType,
      long catalogId,
      long parentId) {
    // determine key prefix, add type if one is passed-in
    String prefixKey =
        entityType == null
            ? this.store.buildPrefixKeyComposite(catalogId, parentId)
            : this.store.buildPrefixKeyComposite(catalogId, parentId, entityType.getCode());
    // check if it has children
    return !this.store.getSliceEntitiesActive().readRange(prefixKey).isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public int lookupEntityGrantRecordsVersionInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId) {
    PolarisBaseEntity entity =
        this.store
            .getSliceEntitiesChangeTracking()
            .read(this.store.buildKeyComposite(catalogId, entityId));

    // does not exist, 0
    return entity == null ? 0 : entity.getGrantRecordsVersion();
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisGrantRecord lookupGrantRecordInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    // lookup the grants records slice to find the usage role
    return this.store
        .getSliceGrantRecords()
        .read(
            this.store.buildKeyComposite(
                securableCatalogId, securableId, granteeCatalogId, granteeId, privilegeCode));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisGrantRecord> loadAllGrantRecordsOnSecurableInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    // now fetch all grants for this securable
    return this.store
        .getSliceGrantRecords()
        .readRange(this.store.buildPrefixKeyComposite(securableCatalogId, securableId));
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull List<PolarisGrantRecord> loadAllGrantRecordsOnGranteeInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    // now fetch all grants assigned to this grantee
    return this.store
        .getSliceGrantRecordsByGrantee()
        .readRange(this.store.buildPrefixKeyComposite(granteeCatalogId, granteeId));
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisPrincipalSecrets loadPrincipalSecretsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    return this.store.getSlicePrincipalSecrets().read(clientId);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PolarisPrincipalSecrets generateNewPrincipalSecretsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull String principalName, long principalId) {
    // ensure principal client id is unique
    PolarisPrincipalSecrets principalSecrets;
    PolarisPrincipalSecrets lookupPrincipalSecrets;
    do {
      // generate new random client id and secrets
      principalSecrets = secretsGenerator.produceSecrets(principalName, principalId);

      // load the existing secrets
      lookupPrincipalSecrets =
          this.store.getSlicePrincipalSecrets().read(principalSecrets.getPrincipalClientId());
    } while (lookupPrincipalSecrets != null);

    // write new principal secrets
    this.store.getSlicePrincipalSecrets().write(principalSecrets);

    // if not found, return null
    return principalSecrets;
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PolarisPrincipalSecrets rotatePrincipalSecretsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {

    // load the existing secrets
    PolarisPrincipalSecrets principalSecrets = this.store.getSlicePrincipalSecrets().read(clientId);

    // should be found
    callCtx
        .getDiagServices()
        .checkNotNull(
            principalSecrets,
            "cannot_find_secrets",
            "client_id={} principalId={}",
            clientId,
            principalId);

    // ensure principal id is matching
    callCtx
        .getDiagServices()
        .check(
            principalId == principalSecrets.getPrincipalId(),
            "principal_id_mismatch",
            "expectedId={} id={}",
            principalId,
            principalSecrets.getPrincipalId());

    // rotate the secrets
    principalSecrets.rotateSecrets(oldSecretHash);
    if (reset) {
      principalSecrets.rotateSecrets(principalSecrets.getMainSecretHash());
    }

    // write back new secrets
    this.store.getSlicePrincipalSecrets().write(principalSecrets);

    // return those
    return principalSecrets;
  }

  /** {@inheritDoc} */
  @Override
  public void deletePrincipalSecretsInCurrentTxn(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    // load the existing secrets
    PolarisPrincipalSecrets principalSecrets = this.store.getSlicePrincipalSecrets().read(clientId);

    // should be found
    callCtx
        .getDiagServices()
        .checkNotNull(
            principalSecrets,
            "cannot_find_secrets",
            "client_id={} principalId={}",
            clientId,
            principalId);

    // ensure principal id is matching
    callCtx
        .getDiagServices()
        .check(
            principalId == principalSecrets.getPrincipalId(),
            "principal_id_mismatch",
            "expectedId={} id={}",
            principalId,
            principalSecrets.getPrincipalId());

    // delete these secrets
    this.store.getSlicePrincipalSecrets().delete(clientId);
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegrationInCurrentTxn(
          @Nonnull PolarisCallContext callCtx,
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    return storageIntegrationProvider.getStorageIntegrationForConfig(
        polarisStorageConfigurationInfo);
  }

  /** {@inheritDoc} */
  @Override
  public @Nullable <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegrationInCurrentTxn(
          @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    PolarisStorageConfigurationInfo storageConfig =
        BaseMetaStoreManager.extractStorageConfiguration(callCtx, entity);
    return storageIntegrationProvider.getStorageIntegrationForConfig(storageConfig);
  }

  @Override
  public void rollback() {
    this.store.rollback();
  }

  @Override
  public @Nonnull PageToken.PageTokenBuilder<?> pageTokenBuilder() {
    return OffsetPageToken.builder();
  }
}
