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
package org.apache.polaris.service.catalog.iceberg;

import static org.apache.polaris.service.exception.IcebergExceptionMapper.isStorageProviderRetryableException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.ws.rs.core.SecurityContext;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewUtil;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.config.BehaviorChangeConfiguration;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.PolarisCredentialVendor;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.service.catalog.SupportsNotifications;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.FileIOUtil;
import org.apache.polaris.service.task.TaskExecutor;
import org.apache.polaris.service.types.NotificationRequest;
import org.apache.polaris.service.types.NotificationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Defines the relationship between PolarisEntities and Iceberg's business logic. */
public class IcebergCatalog extends BaseMetastoreViewCatalog
    implements SupportsNamespaces, SupportsNotifications, Closeable, SupportsCredentialDelegation {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergCatalog.class);

  private static final Joiner SLASH = Joiner.on("/");

  // Config key for whether to allow setting the FILE_IO_IMPL using catalog properties. Should
  // only be allowed in dev/test environments.
  static final String ALLOW_SPECIFYING_FILE_IO_IMPL = "ALLOW_SPECIFYING_FILE_IO_IMPL";
  static final boolean ALLOW_SPECIFYING_FILE_IO_IMPL_DEFAULT = false;

  // Config key for initializing a default "catalogFileIO" that is available either via getIo()
  // or for any TableOperations/ViewOperations instantiated, via ops.io() before entity-specific
  // FileIO initialization is triggered for any such operations.
  // Typically this should only be used in test scenarios where a PolarisIcebergCatalog instance
  // is used for both the "client-side" and "server-side" logic instead of being access through
  // a REST layer.
  static final String INITIALIZE_DEFAULT_CATALOG_FILEIO_FOR_TEST =
      "INITIALIZE_DEFAULT_CATALOG_FILEIO_FOR_TEST";
  static final boolean INITIALIZE_DEFAULT_CATALOG_FILEIO_FOR_TEST_DEFAULT = false;

  public static final Predicate<Exception> SHOULD_RETRY_REFRESH_PREDICATE =
      ex -> {
        // Default arguments from BaseMetastoreTableOperation only stop retries on
        // NotFoundException. We should more carefully identify the set of retriable
        // and non-retriable exceptions here.
        return !(ex instanceof NotFoundException)
            && !(ex instanceof IllegalArgumentException)
            && !(ex instanceof AlreadyExistsException)
            && !(ex instanceof ForbiddenException)
            && !(ex instanceof UnprocessableEntityException)
            && (isStorageProviderRetryableException(ex)
                || isStorageProviderRetryableException(ExceptionUtils.getRootCause(ex)));
      };

  private final PolarisEntityManager entityManager;
  private final CallContext callContext;
  private final PolarisResolutionManifestCatalogView resolvedEntityView;
  private final CatalogEntity catalogEntity;
  private final TaskExecutor taskExecutor;
  private final SecurityContext securityContext;
  private String ioImplClassName;
  private FileIO catalogFileIO;
  private final String catalogName;
  private long catalogId = -1;
  private String defaultBaseLocation;
  private CloseableGroup closeableGroup;
  private Map<String, String> catalogProperties;
  private Map<String, String> tableDefaultProperties;
  private FileIOFactory fileIOFactory;
  private PolarisMetaStoreManager metaStoreManager;
  private TableIdentifier identifier;
  private PolarisEntity entity;

  /**
   * @param entityManager provides handle to underlying PolarisMetaStoreManager with which to
   *     perform mutations on entities.
   * @param callContext the current CallContext
   * @param resolvedEntityView accessor to resolved entity paths that have been pre-vetted to ensure
   *     this catalog instance only interacts with authorized resolved paths.
   * @param taskExecutor Executor we use to register cleanup task handlers
   */
  public IcebergCatalog(
      PolarisEntityManager entityManager,
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      SecurityContext securityContext,
      TaskExecutor taskExecutor,
      FileIOFactory fileIOFactory) {
    this.entityManager = entityManager;
    this.callContext = callContext;
    this.resolvedEntityView = resolvedEntityView;
    this.catalogEntity =
        CatalogEntity.of(resolvedEntityView.getResolvedReferenceCatalogEntity().getRawLeafEntity());
    this.securityContext = securityContext;
    this.taskExecutor = taskExecutor;
    this.catalogId = catalogEntity.getId();
    this.catalogName = catalogEntity.getName();
    this.fileIOFactory = fileIOFactory;
    this.metaStoreManager = metaStoreManager;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @VisibleForTesting
  public FileIO getIo() {
    return catalogFileIO;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    Preconditions.checkState(
        this.catalogName.equals(name),
        "Tried to initialize catalog as name %s but already constructed with name %s",
        name,
        this.catalogName);

    // Ensure catalogProperties is assigned before calling metricsReporter() for proper
    // functionality.
    catalogProperties = properties;

    // Base location from catalogEntity is primary source of truth, otherwise fall through
    // to the same key from the properties map, and finally fall through to WAREHOUSE_LOCATION.
    String baseLocation =
        Optional.ofNullable(catalogEntity.getDefaultBaseLocation())
            .orElse(
                properties.getOrDefault(
                    CatalogEntity.DEFAULT_BASE_LOCATION_KEY,
                    properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "")));
    this.defaultBaseLocation = baseLocation.replaceAll("/*$", "");

    Boolean allowSpecifyingFileIoImpl =
        getBooleanContextConfiguration(
            ALLOW_SPECIFYING_FILE_IO_IMPL, ALLOW_SPECIFYING_FILE_IO_IMPL_DEFAULT);

    PolarisStorageConfigurationInfo storageConfigurationInfo =
        catalogEntity.getStorageConfigurationInfo();
    if (properties.containsKey(CatalogProperties.FILE_IO_IMPL)) {
      ioImplClassName = properties.get(CatalogProperties.FILE_IO_IMPL);

      if (!Boolean.TRUE.equals(allowSpecifyingFileIoImpl)) {
        throw new ValidationException(
            "Cannot set property '%s' to '%s' for this catalog.",
            CatalogProperties.FILE_IO_IMPL, ioImplClassName);
      }
      LOGGER.debug(
          "Allowing overriding ioImplClassName to {} for storageConfiguration {}",
          ioImplClassName,
          storageConfigurationInfo);
    } else {
      if (storageConfigurationInfo != null) {
        ioImplClassName = storageConfigurationInfo.getFileIoImplClassName();
        LOGGER.debug(
            "Resolved ioImplClassName {} from storageConfiguration {}",
            ioImplClassName,
            storageConfigurationInfo);
      } else {
        LOGGER.warn(
            "Cannot resolve property '{}' for null storageConfiguration.",
            CatalogProperties.FILE_IO_IMPL);
      }
    }
    callContext.closeables().addCloseable(this);
    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(metricsReporter());
    closeableGroup.setSuppressCloseFailure(true);

    tableDefaultProperties =
        PropertyUtil.propertiesWithPrefix(properties, CatalogProperties.TABLE_DEFAULT_PREFIX);

    Boolean initializeDefaultCatalogFileioForTest =
        getBooleanContextConfiguration(
            INITIALIZE_DEFAULT_CATALOG_FILEIO_FOR_TEST,
            INITIALIZE_DEFAULT_CATALOG_FILEIO_FOR_TEST_DEFAULT);
    if (Boolean.TRUE.equals(initializeDefaultCatalogFileioForTest)) {
      LOGGER.debug(
          "Initializing a default catalogFileIO with properties {}", tableDefaultProperties);
      this.catalogFileIO = loadFileIO(ioImplClassName, tableDefaultProperties);
      closeableGroup.addCloseable(this.catalogFileIO);
    } else {
      LOGGER.debug("Not initializing default catalogFileIO");
      this.catalogFileIO = null;
    }
  }

  public void setMetaStoreManager(PolarisMetaStoreManager newMetaStoreManager) {
    this.metaStoreManager = newMetaStoreManager;
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }

  @Override
  public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    Preconditions.checkArgument(
        identifier != null && isValidIdentifier(identifier), "Invalid identifier: %s", identifier);
    Preconditions.checkArgument(
        metadataFileLocation != null && !metadataFileLocation.isEmpty(),
        "Cannot register an empty metadata file location as a table");

    int lastSlashIndex = metadataFileLocation.lastIndexOf("/");
    Preconditions.checkArgument(
        lastSlashIndex != -1,
        "Invalid metadata file location; metadata file location must be absolute and contain a '/': %s",
        metadataFileLocation);

    // Throw an exception if this table already exists in the catalog.
    if (tableExists(identifier)) {
      throw new AlreadyExistsException("Table already exists: %s", identifier);
    }

    String locationDir = metadataFileLocation.substring(0, lastSlashIndex);

    TableOperations ops = newTableOps(identifier);

    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(identifier.namespace());
    if (resolvedParent == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved parent for TableIdentifier '%s'", identifier));
    }
    FileIO fileIO =
        loadFileIOForTableLike(
            identifier,
            Set.of(locationDir),
            resolvedParent,
            new HashMap<>(tableDefaultProperties),
            Set.of(PolarisStorageActions.READ));

    InputFile metadataFile = fileIO.newInputFile(metadataFileLocation);
    TableMetadata metadata = TableMetadataParser.read(fileIO, metadataFile);
    ops.commit(null, metadata);

    return new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new PolarisIcebergCatalogTableBuilder(identifier, schema);
  }

  @Override
  public ViewBuilder buildView(TableIdentifier identifier) {
    return new PolarisIcebergCatalogViewBuilder(identifier);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new BasePolarisTableOperations(catalogFileIO, tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    if (tableIdentifier.namespace().isEmpty()) {
      return SLASH.join(
          defaultNamespaceLocation(tableIdentifier.namespace()), tableIdentifier.name());
    } else {
      PolarisResolvedPathWrapper resolvedNamespace =
          resolvedEntityView.getResolvedPath(tableIdentifier.namespace());
      if (resolvedNamespace == null) {
        throw new NoSuchNamespaceException(
            "Namespace does not exist: %s", tableIdentifier.namespace());
      }
      List<PolarisEntity> namespacePath = resolvedNamespace.getRawFullPath();
      String namespaceLocation = resolveLocationForPath(callContext, namespacePath);
      return SLASH.join(namespaceLocation, tableIdentifier.name());
    }
  }

  private String defaultNamespaceLocation(Namespace namespace) {
    if (namespace.isEmpty()) {
      return defaultBaseLocation;
    } else {
      return SLASH.join(defaultBaseLocation, SLASH.join(namespace.levels()));
    }
  }

  private Set<String> getLocationsAllowedToBeAccessed(TableMetadata tableMetadata) {
    Set<String> locations = new HashSet<>();
    locations.add(tableMetadata.location());
    if (tableMetadata
        .properties()
        .containsKey(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY)) {
      locations.add(
          tableMetadata
              .properties()
              .get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY));
    }
    if (tableMetadata
        .properties()
        .containsKey(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY)) {
      locations.add(
          tableMetadata
              .properties()
              .get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY));
    }
    return locations;
  }

  private Set<String> getLocationsAllowedToBeAccessed(ViewMetadata viewMetadata) {
    return Set.of(viewMetadata.location());
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    TableOperations ops = newTableOps(tableIdentifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    Optional<PolarisEntity> storageInfoEntity = findStorageInfo(tableIdentifier);

    // The storageProperties we stash away in the Task should be the superset of the
    // internalProperties of the StorageInfoEntity to be able to use its StorageIntegration
    // combined with other miscellaneous FileIO-related initialization properties defined
    // by the Table.
    Map<String, String> storageProperties =
        storageInfoEntity
            .map(PolarisEntity::getInternalPropertiesAsMap)
            .map(
                properties -> {
                  if (lastMetadata == null) {
                    return Map.<String, String>of();
                  }
                  Map<String, String> clone = new HashMap<>();

                  // The user-configurable table properties are the baseline, but then override
                  // with our restricted properties so that table properties can't clobber the
                  // more restricted ones.
                  clone.putAll(lastMetadata.properties());
                  clone.put(CatalogProperties.FILE_IO_IMPL, ioImplClassName);
                  clone.putAll(properties);
                  clone.put(PolarisTaskConstants.STORAGE_LOCATION, lastMetadata.location());
                  return clone;
                })
            .orElse(Map.of());
    DropEntityResult dropEntityResult =
        dropTableLike(
            PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, storageProperties, purge);
    if (!dropEntityResult.isSuccess()) {
      return false;
    }

    if (purge && lastMetadata != null && dropEntityResult.getCleanupTaskId() != null) {
      LOGGER.info(
          "Scheduled cleanup task {} for table {}",
          dropEntityResult.getCleanupTaskId(),
          tableIdentifier);
      taskExecutor.addTaskHandlerContext(dropEntityResult.getCleanupTaskId(), callContext);
    }

    return true;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(
          "Cannot list tables for namespace. Namespace does not exist: '%s'", namespace);
    }

    return listTableLike(PolarisEntitySubType.ICEBERG_TABLE, namespace);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    renameTableLike(PolarisEntitySubType.ICEBERG_TABLE, from, to);
  }

  @Override
  public void createNamespace(Namespace namespace) {
    createNamespace(namespace, Collections.emptyMap());
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    LOGGER.debug("Creating namespace {} with metadata {}", namespace, metadata);
    if (namespace.isEmpty()) {
      throw new AlreadyExistsException(
          "Cannot create root namespace, as it already exists implicitly.");
    }

    // TODO: These should really be helpers in core Iceberg Namespace.
    Namespace parentNamespace = PolarisCatalogHelpers.getParentNamespace(namespace);

    PolarisResolvedPathWrapper resolvedParent = resolvedEntityView.getResolvedPath(parentNamespace);
    if (resolvedParent == null) {
      throw new NoSuchNamespaceException(
          "Cannot create namespace %s. Parent namespace does not exist.", namespace);
    }
    createNamespaceInternal(namespace, metadata, resolvedParent);
  }

  private void createNamespaceInternal(
      Namespace namespace,
      Map<String, String> metadata,
      PolarisResolvedPathWrapper resolvedParent) {
    String baseLocation = resolveNamespaceLocation(namespace, metadata);
    NamespaceEntity entity =
        new NamespaceEntity.Builder(namespace)
            .setCatalogId(getCatalogId())
            .setId(getMetaStoreManager().generateNewEntityId(getCurrentPolarisContext()).getId())
            .setParentId(resolvedParent.getRawLeafEntity().getId())
            .setProperties(metadata)
            .setCreateTimestamp(System.currentTimeMillis())
            .setBaseLocation(baseLocation)
            .build();
    if (!callContext
        .getPolarisCallContext()
        .getConfigurationStore()
        .getConfiguration(
            callContext.getPolarisCallContext(),
            FeatureConfiguration.ALLOW_NAMESPACE_LOCATION_OVERLAP)) {
      LOGGER.debug("Validating no overlap for {} with sibling tables or namespaces", namespace);
      validateNoLocationOverlap(
          entity.getBaseLocation(), resolvedParent.getRawFullPath(), entity.getName());
    } else {
      LOGGER.debug("Skipping location overlap validation for namespace '{}'", namespace);
    }
    PolarisEntity returnedEntity =
        PolarisEntity.of(
            getMetaStoreManager()
                .createEntityIfNotExists(
                    getCurrentPolarisContext(),
                    PolarisEntity.toCoreList(resolvedParent.getRawFullPath()),
                    entity));
    if (returnedEntity == null) {
      throw new AlreadyExistsException(
          "Cannot create namespace %s. Namespace already exists", namespace);
    }
  }

  private String resolveNamespaceLocation(Namespace namespace, Map<String, String> properties) {
    if (properties.containsKey(PolarisEntityConstants.ENTITY_BASE_LOCATION)) {
      return properties.get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
    } else {
      List<PolarisEntity> parentPath =
          namespace.length() > 1
              ? getResolvedParentNamespace(namespace).getRawFullPath()
              : List.of(resolvedEntityView.getResolvedReferenceCatalogEntity().getRawLeafEntity());

      String parentLocation = resolveLocationForPath(callContext, parentPath);

      return parentLocation + "/" + namespace.level(namespace.length() - 1);
    }
  }

  private static @Nonnull String resolveLocationForPath(
      @Nonnull CallContext callContext, List<PolarisEntity> parentPath) {
    // always take the first object. If it has the base-location, stop there
    AtomicBoolean foundBaseLocation = new AtomicBoolean(false);
    return parentPath.reversed().stream()
        .takeWhile(
            entity ->
                !foundBaseLocation.getAndSet(
                    entity
                        .getPropertiesAsMap()
                        .containsKey(PolarisEntityConstants.ENTITY_BASE_LOCATION)))
        .toList()
        .reversed()
        .stream()
        .map(entity -> baseLocation(callContext, entity))
        .map(IcebergCatalog::stripLeadingTrailingSlash)
        .collect(Collectors.joining("/"));
  }

  private static @Nullable String baseLocation(
      @Nonnull CallContext callContext, PolarisEntity entity) {
    if (entity.getType().equals(PolarisEntityType.CATALOG)) {
      CatalogEntity catEntity = CatalogEntity.of(entity);
      String catalogDefaultBaseLocation = catEntity.getDefaultBaseLocation();
      callContext
          .getPolarisCallContext()
          .getDiagServices()
          .checkNotNull(
              catalogDefaultBaseLocation,
              "Tried to resolve location with catalog with null default base location",
              "catalog = {}",
              catEntity);
      return catalogDefaultBaseLocation;
    } else {
      String baseLocation =
          entity.getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
      if (baseLocation != null) {
        return baseLocation;
      } else {
        String entityName = entity.getName();
        callContext
            .getPolarisCallContext()
            .getDiagServices()
            .checkNotNull(
                entityName,
                "Tried to resolve location with entity without base location or name",
                "entity = {}",
                entity);
        return entityName;
      }
    }
  }

  private static String stripLeadingTrailingSlash(String location) {
    if (location.startsWith("/")) {
      return stripLeadingTrailingSlash(location.substring(1));
    }
    if (location.endsWith("/")) {
      return location.substring(0, location.length() - 1);
    } else {
      return location;
    }
  }

  private PolarisResolvedPathWrapper getResolvedParentNamespace(Namespace namespace) {
    Namespace parentNamespace =
        Namespace.of(Arrays.copyOf(namespace.levels(), namespace.length() - 1));
    PolarisResolvedPathWrapper resolvedParent = resolvedEntityView.getResolvedPath(parentNamespace);
    if (resolvedParent == null) {
      return resolvedEntityView.getPassthroughResolvedPath(parentNamespace);
    }
    return resolvedParent;
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return Optional.ofNullable(namespace)
        .filter(ns -> !ns.isEmpty())
        .map(resolvedEntityView::getResolvedPath)
        .isPresent();
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    if (namespace.isEmpty()) {
      throw new IllegalArgumentException("Cannot drop root namespace");
    }
    PolarisResolvedPathWrapper resolvedEntities = resolvedEntityView.getResolvedPath(namespace);
    if (resolvedEntities == null) {
      return false;
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    PolarisEntity leafEntity = resolvedEntities.getRawLeafEntity();

    // drop if exists and is empty
    PolarisCallContext polarisCallContext = callContext.getPolarisCallContext();
    DropEntityResult dropEntityResult =
        getMetaStoreManager()
            .dropEntityIfExists(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(catalogPath),
                leafEntity,
                Map.of(),
                polarisCallContext
                    .getConfigurationStore()
                    .getConfiguration(
                        polarisCallContext, FeatureConfiguration.CLEANUP_ON_NAMESPACE_DROP));

    if (!dropEntityResult.isSuccess() && dropEntityResult.failedBecauseNotEmpty()) {
      throw new NamespaceNotEmptyException("Namespace %s is not empty", namespace);
    }

    // return status of drop operation
    return dropEntityResult.isSuccess();
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    PolarisResolvedPathWrapper resolvedEntities = resolvedEntityView.getResolvedPath(namespace);
    if (resolvedEntities == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    PolarisEntity entity = resolvedEntities.getRawLeafEntity();
    Map<String, String> newProperties = new HashMap<>(entity.getPropertiesAsMap());

    // Merge new properties into existing map.
    newProperties.putAll(properties);
    PolarisEntity updatedEntity =
        new PolarisEntity.Builder(entity).setProperties(newProperties).build();

    if (!callContext
        .getPolarisCallContext()
        .getConfigurationStore()
        .getConfiguration(
            callContext.getPolarisCallContext(),
            FeatureConfiguration.ALLOW_NAMESPACE_LOCATION_OVERLAP)) {
      LOGGER.debug("Validating no overlap with sibling tables or namespaces");
      validateNoLocationOverlap(
          NamespaceEntity.of(updatedEntity).getBaseLocation(),
          resolvedEntities.getRawParentPath(),
          updatedEntity.getName());
    } else {
      LOGGER.debug("Skipping location overlap validation for namespace '{}'", namespace);
    }

    List<PolarisEntity> parentPath = resolvedEntities.getRawFullPath();
    PolarisEntity returnedEntity =
        Optional.ofNullable(
                getMetaStoreManager()
                    .updateEntityPropertiesIfNotChanged(
                        getCurrentPolarisContext(),
                        PolarisEntity.toCoreList(parentPath),
                        updatedEntity)
                    .getEntity())
            .map(PolarisEntity::new)
            .orElse(null);
    if (returnedEntity == null) {
      throw new RuntimeException("Concurrent modification of namespace: " + namespace);
    }
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    PolarisResolvedPathWrapper resolvedEntities = resolvedEntityView.getResolvedPath(namespace);
    if (resolvedEntities == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    PolarisEntity entity = resolvedEntities.getRawLeafEntity();

    Map<String, String> updatedProperties = new HashMap<>(entity.getPropertiesAsMap());
    properties.forEach(updatedProperties::remove);

    PolarisEntity updatedEntity =
        new PolarisEntity.Builder(entity).setProperties(updatedProperties).build();

    List<PolarisEntity> parentPath = resolvedEntities.getRawFullPath();
    PolarisEntity returnedEntity =
        Optional.ofNullable(
                getMetaStoreManager()
                    .updateEntityPropertiesIfNotChanged(
                        getCurrentPolarisContext(),
                        PolarisEntity.toCoreList(parentPath),
                        updatedEntity)
                    .getEntity())
            .map(PolarisEntity::new)
            .orElse(null);
    if (returnedEntity == null) {
      throw new RuntimeException("Concurrent modification of namespace: " + namespace);
    }
    return true;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    PolarisResolvedPathWrapper resolvedEntities = resolvedEntityView.getResolvedPath(namespace);
    if (resolvedEntities == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    NamespaceEntity entity = NamespaceEntity.of(resolvedEntities.getRawLeafEntity());
    Preconditions.checkState(
        entity.getParentNamespace().equals(PolarisCatalogHelpers.getParentNamespace(namespace)),
        "Mismatched stored parentNamespace '%s' vs looked up parentNamespace '%s",
        entity.getParentNamespace(),
        PolarisCatalogHelpers.getParentNamespace(namespace));

    return entity.getPropertiesAsMap();
  }

  @Override
  public List<Namespace> listNamespaces() {
    return listNamespaces(Namespace.empty());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    PolarisResolvedPathWrapper resolvedEntities = resolvedEntityView.getResolvedPath(namespace);
    if (resolvedEntities == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawFullPath();
    List<PolarisEntity.NameAndId> entities =
        PolarisEntity.toNameAndIdList(
            getMetaStoreManager()
                .listEntities(
                    getCurrentPolarisContext(),
                    PolarisEntity.toCoreList(catalogPath),
                    PolarisEntityType.NAMESPACE,
                    PolarisEntitySubType.NULL_SUBTYPE)
                .getEntities());
    return PolarisCatalogHelpers.nameAndIdToNamespaces(catalogPath, entities);
  }

  @Override
  public void close() throws IOException {
    if (closeableGroup != null) {
      closeableGroup.close();
    }
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(
          "Cannot list views for namespace. Namespace does not exist: '%s'", namespace);
    }

    return listTableLike(PolarisEntitySubType.ICEBERG_VIEW, namespace);
  }

  @Override
  protected ViewOperations newViewOps(TableIdentifier identifier) {
    return new BasePolarisViewOperations(catalogFileIO, identifier);
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    return dropTableLike(PolarisEntitySubType.ICEBERG_VIEW, identifier, Map.of(), true).isSuccess();
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    renameTableLike(PolarisEntitySubType.ICEBERG_VIEW, from, to);
  }

  @Override
  public boolean sendNotification(
      TableIdentifier identifier, NotificationRequest notificationRequest) {
    return sendNotificationForTableLike(
        PolarisEntitySubType.ICEBERG_TABLE, identifier, notificationRequest);
  }

  @Override
  public Map<String, String> getCredentialConfig(
      TableIdentifier tableIdentifier,
      TableMetadata tableMetadata,
      Set<PolarisStorageActions> storageActions) {
    Optional<PolarisEntity> storageInfo = findStorageInfo(tableIdentifier);
    if (storageInfo.isEmpty()) {
      LOGGER
          .atWarn()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .log("Table entity has no storage configuration in its hierarchy");
      return Map.of();
    }
    return FileIOUtil.refreshCredentials(
        callContext,
        entityManager,
        getCredentialVendor(),
        callContext.getPolarisCallContext().getConfigurationStore(),
        tableIdentifier,
        getLocationsAllowedToBeAccessed(tableMetadata),
        storageActions,
        storageInfo.get());
  }

  /**
   * Based on configuration settings, for callsites that need to handle potentially setting a new
   * base location for a TableLike entity, produces the transformed location if applicable, or else
   * the unaltered specified location.
   */
  public String transformTableLikeLocation(String specifiedTableLikeLocation) {
    String replaceNewLocationPrefix = catalogEntity.getReplaceNewLocationPrefixWithCatalogDefault();
    if (specifiedTableLikeLocation != null
        && replaceNewLocationPrefix != null
        && specifiedTableLikeLocation.startsWith(replaceNewLocationPrefix)) {
      String modifiedLocation =
          defaultBaseLocation
              + specifiedTableLikeLocation.substring(replaceNewLocationPrefix.length());
      LOGGER
          .atDebug()
          .addKeyValue("specifiedTableLikeLocation", specifiedTableLikeLocation)
          .addKeyValue("modifiedLocation", modifiedLocation)
          .log("Translating specifiedTableLikeLocation based on config");
      return modifiedLocation;
    }
    return specifiedTableLikeLocation;
  }

  private @Nonnull Optional<PolarisEntity> findStorageInfo(TableIdentifier tableIdentifier) {
    PolarisResolvedPathWrapper resolvedTableEntities =
        resolvedEntityView.getResolvedPath(
            tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE);

    PolarisResolvedPathWrapper resolvedStorageEntity =
        resolvedTableEntities == null
            ? resolvedEntityView.getResolvedPath(tableIdentifier.namespace())
            : resolvedTableEntities;

    return FileIOUtil.findStorageInfoFromHierarchy(resolvedStorageEntity);
  }

  /**
   * Validates that the specified {@code location} is valid for whatever storage config is found for
   * this TableLike's parent hierarchy.
   */
  private void validateLocationForTableLike(TableIdentifier identifier, String location) {
    PolarisResolvedPathWrapper resolvedStorageEntity =
        resolvedEntityView.getResolvedPath(
            identifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ANY_SUBTYPE);
    if (resolvedStorageEntity == null) {
      resolvedStorageEntity = resolvedEntityView.getResolvedPath(identifier.namespace());
    }
    if (resolvedStorageEntity == null) {
      resolvedStorageEntity = resolvedEntityView.getPassthroughResolvedPath(identifier.namespace());
    }

    validateLocationForTableLike(identifier, location, resolvedStorageEntity);
  }

  /**
   * Validates that the specified {@code location} is valid for whatever storage config is found for
   * this TableLike's parent hierarchy.
   */
  private void validateLocationForTableLike(
      TableIdentifier identifier,
      String location,
      PolarisResolvedPathWrapper resolvedStorageEntity) {
    validateLocationsForTableLike(identifier, Set.of(location), resolvedStorageEntity);
  }

  /**
   * Validates that the specified {@code locations} are valid for whatever storage config is found
   * for this TableLike's parent hierarchy.
   */
  private void validateLocationsForTableLike(
      TableIdentifier identifier,
      Set<String> locations,
      PolarisResolvedPathWrapper resolvedStorageEntity) {
    Optional<PolarisStorageConfigurationInfo> optStorageConfiguration =
        PolarisStorageConfigurationInfo.forEntityPath(
            callContext.getPolarisCallContext().getDiagServices(),
            resolvedStorageEntity.getRawFullPath());

    optStorageConfiguration.ifPresentOrElse(
        storageConfigInfo -> {
          Map<String, Map<PolarisStorageActions, PolarisStorageIntegration.ValidationResult>>
              validationResults =
                  InMemoryStorageIntegration.validateSubpathsOfAllowedLocations(
                      storageConfigInfo, Set.of(PolarisStorageActions.ALL), locations);
          validationResults
              .values()
              .forEach(
                  actionResult ->
                      actionResult
                          .values()
                          .forEach(
                              result -> {
                                if (!result.isSuccess()) {
                                  throw new ForbiddenException(
                                      "Invalid locations '%s' for identifier '%s': %s",
                                      locations, identifier, result.getMessage());
                                } else {
                                  LOGGER.debug(
                                      "Validated locations '{}' for identifier '{}'",
                                      locations,
                                      identifier);
                                }
                              }));

          // TODO: Consider exposing a property to control whether to use the explicit default
          // in-memory PolarisStorageIntegration implementation to perform validation or
          // whether to delegate to PolarisMetaStoreManager::validateAccessToLocations.
          // Usually the validation is better to perform with local business logic, but if
          // there are additional rules to be evaluated by a custom PolarisMetaStoreManager
          // implementation, then the validation should go through that API instead as follows:
          //
          // PolarisMetaStoreManager.ValidateAccessResult validateResult =
          //     getMetaStoreManager().validateAccessToLocations(
          //         getCurrentPolarisContext(),
          //         storageInfoHolderEntity.getCatalogId(),
          //         storageInfoHolderEntity.getId(),
          //         Set.of(PolarisStorageActions.ALL),
          //         Set.of(location));
          // if (!validateResult.isSuccess()) {
          //   throw new ForbiddenException("Invalid location '%s' for identifier '%s': %s",
          //       location, identifier, validateResult.getExtraInformation());
          // }
        },
        () -> {
          List<String> allowedStorageTypes =
              callContext
                  .getPolarisCallContext()
                  .getConfigurationStore()
                  .getConfiguration(
                      callContext.getPolarisCallContext(),
                      FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES);
          if (!allowedStorageTypes.contains(StorageConfigInfo.StorageTypeEnum.FILE.name())) {
            List<String> invalidLocations =
                locations.stream()
                    .filter(location -> location.startsWith("file:") || location.startsWith("http"))
                    .collect(Collectors.toList());
            if (!invalidLocations.isEmpty()) {
              throw new ForbiddenException(
                  "Invalid locations '%s' for identifier '%s': File locations are not allowed",
                  invalidLocations, identifier);
            }
          }
        });
  }

  /**
   * Validates the table location has no overlap with other entities after checking the
   * configuration of the service
   */
  private void validateNoLocationOverlap(
      CatalogEntity catalog,
      TableIdentifier identifier,
      List<PolarisEntity> resolvedNamespace,
      String location,
      PolarisEntity entity) {
    boolean validateViewOverlap =
        callContext
            .getPolarisCallContext()
            .getConfigurationStore()
            .getConfiguration(
                callContext.getPolarisCallContext(),
                BehaviorChangeConfiguration.VALIDATE_VIEW_LOCATION_OVERLAP);

    if (callContext
        .getPolarisCallContext()
        .getConfigurationStore()
        .getConfiguration(
            callContext.getPolarisCallContext(),
            catalog,
            FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP)) {
      LOGGER.debug("Skipping location overlap validation for identifier '{}'", identifier);
    } else if (validateViewOverlap
        || entity.getSubType().equals(PolarisEntitySubType.ICEBERG_TABLE)) {
      LOGGER.debug("Validating no overlap with sibling tables or namespaces");
      validateNoLocationOverlap(location, resolvedNamespace, identifier.name());
    }
  }

  /**
   * Validate no location overlap exists between the entity path and its sibling entities. This
   * resolves all siblings at the same level as the target entity (namespaces if the target entity
   * is a namespace whose parent is the catalog, namespaces and tables otherwise) and checks the
   * base-location property of each. The target entity's base location may not be a prefix or a
   * suffix of any sibling entity's base location.
   */
  private void validateNoLocationOverlap(
      String location, List<PolarisEntity> parentPath, String name) {
    ListEntitiesResult siblingNamespacesResult =
        getMetaStoreManager()
            .listEntities(
                callContext.getPolarisCallContext(),
                parentPath.stream().map(PolarisEntity::toCore).collect(Collectors.toList()),
                PolarisEntityType.NAMESPACE,
                PolarisEntitySubType.ANY_SUBTYPE);
    if (!siblingNamespacesResult.isSuccess()) {
      throw new IllegalStateException(
          "Unable to resolve siblings entities to validate location - could not list namespaces");
    }

    // if the entity path has more than just the catalog, check for tables as well as other
    // namespaces
    Optional<NamespaceEntity> parentNamespace =
        parentPath.size() > 1
            ? Optional.of(NamespaceEntity.of(parentPath.getLast()))
            : Optional.empty();

    List<TableIdentifier> siblingTables =
        parentNamespace
            .map(
                ns -> {
                  ListEntitiesResult siblingTablesResult =
                      getMetaStoreManager()
                          .listEntities(
                              callContext.getPolarisCallContext(),
                              parentPath.stream()
                                  .map(PolarisEntity::toCore)
                                  .collect(Collectors.toList()),
                              PolarisEntityType.TABLE_LIKE,
                              PolarisEntitySubType.ANY_SUBTYPE);
                  if (!siblingTablesResult.isSuccess()) {
                    throw new IllegalStateException(
                        "Unable to resolve siblings entities to validate location - could not list tables");
                  }
                  return siblingTablesResult.getEntities().stream()
                      .map(tbl -> TableIdentifier.of(ns.asNamespace(), tbl.getName()))
                      .collect(Collectors.toList());
                })
            .orElse(List.of());

    List<Namespace> siblingNamespaces =
        siblingNamespacesResult.getEntities().stream()
            .map(
                ns -> {
                  String[] nsLevels =
                      parentNamespace
                          .map(parent -> parent.asNamespace().levels())
                          .orElse(new String[0]);
                  String[] newLevels = Arrays.copyOf(nsLevels, nsLevels.length + 1);
                  newLevels[nsLevels.length] = ns.getName();
                  return Namespace.of(newLevels);
                })
            .toList();
    LOGGER.debug(
        "Resolving {} sibling entities to validate location",
        siblingTables.size() + siblingNamespaces.size());
    PolarisResolutionManifest resolutionManifest =
        new PolarisResolutionManifest(
            callContext, entityManager, securityContext, parentPath.getFirst().getName());
    siblingTables.forEach(
        tbl ->
            resolutionManifest.addPath(
                new ResolverPath(
                    PolarisCatalogHelpers.tableIdentifierToList(tbl), PolarisEntityType.TABLE_LIKE),
                tbl));
    siblingNamespaces.forEach(
        ns ->
            resolutionManifest.addPath(
                new ResolverPath(Arrays.asList(ns.levels()), PolarisEntityType.NAMESPACE), ns));
    ResolverStatus status = resolutionManifest.resolveAll();
    if (!status.getStatus().equals(ResolverStatus.StatusEnum.SUCCESS)) {
      String message =
          "Unable to resolve sibling entities to validate location - " + status.getStatus();
      if (status.getStatus().equals(ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED)) {
        message += ". Could not resolve entity: " + status.getFailedToResolvedEntityName();
      }
      throw new IllegalStateException(message);
    }

    StorageLocation targetLocation = StorageLocation.of(location);
    Stream.concat(
            siblingTables.stream()
                .filter(tbl -> !tbl.name().equals(name))
                .map(
                    tbl -> {
                      PolarisResolvedPathWrapper resolveTablePath =
                          resolutionManifest.getResolvedPath(tbl);
                      return IcebergTableLikeEntity.of(resolveTablePath.getRawLeafEntity())
                          .getBaseLocation();
                    }),
            siblingNamespaces.stream()
                .filter(ns -> !ns.level(ns.length() - 1).equals(name))
                .map(
                    ns -> {
                      PolarisResolvedPathWrapper resolveNamespacePath =
                          resolutionManifest.getResolvedPath(ns);
                      return NamespaceEntity.of(resolveNamespacePath.getRawLeafEntity())
                          .getBaseLocation();
                    }))
        .filter(java.util.Objects::nonNull)
        .map(StorageLocation::of)
        .forEach(
            siblingLocation -> {
              if (targetLocation.isChildOf(siblingLocation)
                  || siblingLocation.isChildOf(targetLocation)) {
                throw new org.apache.iceberg.exceptions.ForbiddenException(
                    "Unable to create table at location '%s' because it conflicts with existing table or namespace at location '%s'",
                    targetLocation, siblingLocation);
              }
            });
  }

  private class PolarisIcebergCatalogTableBuilder
      extends BaseMetastoreViewCatalog.BaseMetastoreViewCatalogTableBuilder {

    public PolarisIcebergCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
    }

    @Override
    public TableBuilder withLocation(String newLocation) {
      return super.withLocation(transformTableLikeLocation(newLocation));
    }
  }

  private class PolarisIcebergCatalogViewBuilder extends BaseMetastoreViewCatalog.BaseViewBuilder {

    public PolarisIcebergCatalogViewBuilder(TableIdentifier identifier) {
      super(identifier);
      withProperties(
          PropertyUtil.propertiesWithPrefix(IcebergCatalog.this.properties(), "table-default."));
    }

    @Override
    public ViewBuilder withLocation(String newLocation) {
      return super.withLocation(transformTableLikeLocation(newLocation));
    }
  }

  private class BasePolarisTableOperations extends BaseMetastoreTableOperations {
    private final TableIdentifier tableIdentifier;
    private final String fullTableName;
    private FileIO tableFileIO;

    BasePolarisTableOperations(FileIO defaultFileIO, TableIdentifier tableIdentifier) {
      LOGGER.debug("new BasePolarisTableOperations for {}", tableIdentifier);
      this.tableIdentifier = tableIdentifier;
      this.fullTableName = fullTableName(catalogName, tableIdentifier);
      this.tableFileIO = defaultFileIO;
    }

    @Override
    public void doRefresh() {
      LOGGER.debug("doRefresh for tableIdentifier {}", tableIdentifier);
      // While doing refresh/commit protocols, we must fetch the fresh "passthrough" resolved
      // table entity instead of the statically-resolved authz resolution set.
      PolarisResolvedPathWrapper resolvedEntities =
          resolvedEntityView.getPassthroughResolvedPath(
              tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE);
      IcebergTableLikeEntity entity = null;

      if (resolvedEntities != null) {
        entity = IcebergTableLikeEntity.of(resolvedEntities.getRawLeafEntity());
        if (!tableIdentifier.equals(entity.getTableIdentifier())) {
          LOGGER
              .atError()
              .addKeyValue("entity.getTableIdentifier()", entity.getTableIdentifier())
              .addKeyValue("tableIdentifier", tableIdentifier)
              .log("Stored table identifier mismatches requested identifier");
        }
      }

      String latestLocation = entity != null ? entity.getMetadataLocation() : null;
      LOGGER.debug("Refreshing latestLocation: {}", latestLocation);
      if (latestLocation == null) {
        disableRefresh();
      } else {
        refreshFromMetadataLocation(
            latestLocation,
            SHOULD_RETRY_REFRESH_PREDICATE,
            getMaxMetadataRefreshRetries(),
            metadataLocation -> {
              String latestLocationDir =
                  latestLocation.substring(0, latestLocation.lastIndexOf('/'));
              // TODO: Once we have the "current" table properties pulled into the resolvedEntity
              // then we should use the actual current table properties for IO refresh here
              // instead of the general tableDefaultProperties.
              FileIO fileIO =
                  loadFileIOForTableLike(
                      tableIdentifier,
                      Set.of(latestLocationDir),
                      resolvedEntities,
                      new HashMap<>(tableDefaultProperties),
                      Set.of(PolarisStorageActions.READ));
              return TableMetadataParser.read(fileIO, metadataLocation);
            });
      }
    }

    @Override
    public void doCommit(TableMetadata base, TableMetadata metadata) {
      LOGGER.debug(
          "doCommit for table {} with base {}, metadata {}", tableIdentifier, base, metadata);
      // TODO: Maybe avoid writing metadata if there's definitely a transaction conflict
      if (null == base && !namespaceExists(tableIdentifier.namespace())) {
        throw new NoSuchNamespaceException(
            "Cannot create table '%s'. Namespace does not exist: '%s'",
            tableIdentifier, tableIdentifier.namespace());
      }

      PolarisResolvedPathWrapper resolvedTableEntities =
          resolvedEntityView.getPassthroughResolvedPath(
              tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE);

      // Fetch credentials for the resolved entity. The entity could be the table itself (if it has
      // already been stored and credentials have been configured directly) or it could be the
      // table's namespace or catalog.
      PolarisResolvedPathWrapper resolvedStorageEntity =
          resolvedTableEntities == null
              ? resolvedEntityView.getResolvedPath(tableIdentifier.namespace())
              : resolvedTableEntities;

      // refresh credentials because we need to read the metadata file to validate its location
      tableFileIO =
          loadFileIOForTableLike(
              tableIdentifier,
              getLocationsAllowedToBeAccessed(metadata),
              resolvedStorageEntity,
              new HashMap<>(metadata.properties()),
              Set.of(PolarisStorageActions.READ, PolarisStorageActions.WRITE));

      List<PolarisEntity> resolvedNamespace =
          resolvedTableEntities == null
              ? resolvedEntityView.getResolvedPath(tableIdentifier.namespace()).getRawFullPath()
              : resolvedTableEntities.getRawParentPath();
      CatalogEntity catalog = CatalogEntity.of(resolvedNamespace.getFirst());

      if (base == null
          || !metadata.location().equals(base.location())
          || !Objects.equal(
              base.properties().get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY),
              metadata
                  .properties()
                  .get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY))) {
        // If location is changing then we must validate that the requested location is valid
        // for the storage configuration inherited under this entity's path.
        Set<String> dataLocations = new HashSet<>();
        dataLocations.add(metadata.location());
        if (metadata.properties().get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY)
            != null) {
          dataLocations.add(
              metadata
                  .properties()
                  .get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY));
        }
        if (metadata
                .properties()
                .get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY)
            != null) {
          dataLocations.add(
              metadata
                  .properties()
                  .get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY));
        }
        validateLocationsForTableLike(tableIdentifier, dataLocations, resolvedStorageEntity);
        // also validate that the table location doesn't overlap an existing table
        dataLocations.forEach(
            location ->
                validateNoLocationOverlap(
                    catalogEntity,
                    tableIdentifier,
                    resolvedNamespace,
                    location,
                    resolvedStorageEntity.getRawLeafEntity()));
        // and that the metadata file points to a location within the table's directory structure
        if (metadata.metadataFileLocation() != null) {
          validateMetadataFileInTableDir(tableIdentifier, metadata, catalog);
        }
      }

      String newLocation = writeNewMetadataIfRequired(base == null, metadata);
      String oldLocation = base == null ? null : base.metadataFileLocation();

      // TODO: Consider using the entity from doRefresh() directly to do the conflict detection
      // instead of a two-layer CAS (checking metadataLocation to detect concurrent modification
      // between doRefresh() and doCommit(), and then updateEntityPropertiesIfNotChanged to detect
      // concurrent
      // modification between our checking of unchanged metadataLocation here and actual
      // persistence-layer commit).
      PolarisResolvedPathWrapper resolvedPath =
          resolvedEntityView.getPassthroughResolvedPath(
              tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ANY_SUBTYPE);
      if (resolvedPath != null && resolvedPath.getRawLeafEntity() != null) {
        if (resolvedPath.getRawLeafEntity().getSubType() == PolarisEntitySubType.ICEBERG_VIEW) {
          throw new AlreadyExistsException(
              "View with same name already exists: %s", tableIdentifier);
        } else if (resolvedPath.getRawLeafEntity().getSubType()
            == PolarisEntitySubType.GENERIC_TABLE) {
          throw new AlreadyExistsException(
              "Generic table with same name already exists: %s", tableIdentifier);
        }
      }
      IcebergTableLikeEntity entity =
          IcebergTableLikeEntity.of(resolvedPath == null ? null : resolvedPath.getRawLeafEntity());
      String existingLocation;
      if (null == entity) {
        existingLocation = null;
        entity =
            new IcebergTableLikeEntity.Builder(tableIdentifier, newLocation)
                .setCatalogId(getCatalogId())
                .setSubType(PolarisEntitySubType.ICEBERG_TABLE)
                .setBaseLocation(metadata.location())
                .setId(
                    getMetaStoreManager().generateNewEntityId(getCurrentPolarisContext()).getId())
                .build();
      } else {
        existingLocation = entity.getMetadataLocation();
        entity =
            new IcebergTableLikeEntity.Builder(entity)
                .setBaseLocation(metadata.location())
                .setMetadataLocation(newLocation)
                .build();
      }
      if (!Objects.equal(existingLocation, oldLocation)) {
        if (null == base) {
          throw new AlreadyExistsException("Table already exists: %s", tableName());
        }

        if (null == existingLocation) {
          throw new NoSuchTableException("Table does not exist: %s", tableName());
        }

        throw new CommitFailedException(
            "Cannot commit to table %s metadata location from %s to %s "
                + "because it has been concurrently modified to %s",
            tableIdentifier, oldLocation, newLocation, existingLocation);
      }
      if (null == existingLocation) {
        createTableLike(tableIdentifier, entity);
      } else {
        updateTableLike(tableIdentifier, entity);
      }
    }

    @Override
    public FileIO io() {
      return tableFileIO;
    }

    @Override
    protected String tableName() {
      return fullTableName;
    }
  }

  private void validateMetadataFileInTableDir(
      TableIdentifier identifier, TableMetadata metadata, CatalogEntity catalog) {
    PolarisCallContext polarisCallContext = callContext.getPolarisCallContext();
    boolean allowEscape =
        polarisCallContext
            .getConfigurationStore()
            .getConfiguration(
                polarisCallContext, FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION);
    if (!allowEscape
        && !polarisCallContext
            .getConfigurationStore()
            .getConfiguration(
                polarisCallContext, FeatureConfiguration.ALLOW_EXTERNAL_METADATA_FILE_LOCATION)) {
      LOGGER.debug(
          "Validating base location {} for table {} in metadata file {}",
          metadata.location(),
          identifier,
          metadata.metadataFileLocation());
      StorageLocation metadataFileLocation = StorageLocation.of(metadata.metadataFileLocation());
      StorageLocation baseLocation = StorageLocation.of(metadata.location());
      if (!metadataFileLocation.isChildOf(baseLocation)) {
        throw new BadRequestException(
            "Metadata location %s is not allowed outside of table location %s",
            metadata.metadataFileLocation(), metadata.location());
      }
    }
  }

  private class BasePolarisViewOperations extends BaseViewOperations {
    private final TableIdentifier identifier;
    private final String fullViewName;
    private FileIO viewFileIO;

    BasePolarisViewOperations(FileIO defaultFileIO, TableIdentifier identifier) {
      this.viewFileIO = defaultFileIO;
      this.identifier = identifier;
      this.fullViewName = ViewUtil.fullViewName(catalogName, identifier);
    }

    @Override
    public void doRefresh() {
      PolarisResolvedPathWrapper resolvedEntities =
          resolvedEntityView.getPassthroughResolvedPath(
              identifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_VIEW);
      IcebergTableLikeEntity entity = null;

      if (resolvedEntities != null) {
        entity = IcebergTableLikeEntity.of(resolvedEntities.getRawLeafEntity());
        if (!identifier.equals(entity.getTableIdentifier())) {
          LOGGER
              .atError()
              .addKeyValue("entity.getTableIdentifier()", entity.getTableIdentifier())
              .addKeyValue("identifier", identifier)
              .log("Stored view identifier mismatches requested identifier");
        }
      }

      String latestLocation = entity != null ? entity.getMetadataLocation() : null;
      LOGGER.debug("Refreshing view latestLocation: {}", latestLocation);
      if (latestLocation == null) {
        disableRefresh();
      } else {
        refreshFromMetadataLocation(
            latestLocation,
            SHOULD_RETRY_REFRESH_PREDICATE,
            getMaxMetadataRefreshRetries(),
            metadataLocation -> {
              String latestLocationDir =
                  latestLocation.substring(0, latestLocation.lastIndexOf('/'));

              // TODO: Once we have the "current" table properties pulled into the resolvedEntity
              // then we should use the actual current table properties for IO refresh here
              // instead of the general tableDefaultProperties.
              FileIO fileIO =
                  loadFileIOForTableLike(
                      identifier,
                      Set.of(latestLocationDir),
                      resolvedEntities,
                      new HashMap<>(tableDefaultProperties),
                      Set.of(PolarisStorageActions.READ));

              return ViewMetadataParser.read(fileIO.newInputFile(metadataLocation));
            });
      }
    }

    @Override
    public void doCommit(ViewMetadata base, ViewMetadata metadata) {
      // TODO: Maybe avoid writing metadata if there's definitely a transaction conflict
      LOGGER.debug("doCommit for view {} with base {}, metadata {}", identifier, base, metadata);
      if (null == base && !namespaceExists(identifier.namespace())) {
        throw new NoSuchNamespaceException(
            "Cannot create view '%s'. Namespace does not exist: '%s'",
            identifier, identifier.namespace());
      }

      PolarisResolvedPathWrapper resolvedTable =
          resolvedEntityView.getPassthroughResolvedPath(
              identifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE);
      if (resolvedTable != null) {
        throw new AlreadyExistsException("Table with same name already exists: %s", identifier);
      }

      PolarisResolvedPathWrapper resolvedEntities =
          resolvedEntityView.getPassthroughResolvedPath(
              identifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_VIEW);

      // Fetch credentials for the resolved entity. The entity could be the view itself (if it has
      // already been stored and credentials have been configured directly) or it could be the
      // table's namespace or catalog.
      PolarisResolvedPathWrapper resolvedStorageEntity =
          resolvedEntities == null
              ? resolvedEntityView.getResolvedPath(identifier.namespace())
              : resolvedEntities;

      List<PolarisEntity> resolvedNamespace =
          resolvedEntities == null
              ? resolvedEntityView.getResolvedPath(identifier.namespace()).getRawFullPath()
              : resolvedEntities.getRawParentPath();
      if (base == null || !metadata.location().equals(base.location())) {
        // If location is changing then we must validate that the requested location is valid
        // for the storage configuration inherited under this entity's path.
        validateLocationForTableLike(identifier, metadata.location(), resolvedStorageEntity);
        validateNoLocationOverlap(
            catalogEntity,
            identifier,
            resolvedNamespace,
            metadata.location(),
            resolvedStorageEntity.getRawLeafEntity());
      }

      Map<String, String> tableProperties = new HashMap<>(metadata.properties());

      viewFileIO =
          loadFileIOForTableLike(
              identifier,
              getLocationsAllowedToBeAccessed(metadata),
              resolvedStorageEntity,
              tableProperties,
              Set.of(PolarisStorageActions.READ, PolarisStorageActions.WRITE));

      String newLocation = writeNewMetadataIfRequired(metadata);
      String oldLocation = base == null ? null : currentMetadataLocation();

      IcebergTableLikeEntity entity =
          IcebergTableLikeEntity.of(
              resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());
      String existingLocation;
      if (null == entity) {
        existingLocation = null;
        entity =
            new IcebergTableLikeEntity.Builder(identifier, newLocation)
                .setCatalogId(getCatalogId())
                .setSubType(PolarisEntitySubType.ICEBERG_VIEW)
                .setId(
                    getMetaStoreManager().generateNewEntityId(getCurrentPolarisContext()).getId())
                .build();
      } else {
        existingLocation = entity.getMetadataLocation();
        entity =
            new IcebergTableLikeEntity.Builder(entity).setMetadataLocation(newLocation).build();
      }
      if (!Objects.equal(existingLocation, oldLocation)) {
        if (null == base) {
          throw new AlreadyExistsException("View already exists: %s", identifier);
        }

        if (null == existingLocation) {
          throw new NoSuchViewException("View does not exist: %s", identifier);
        }

        throw new CommitFailedException(
            "Cannot commit to view %s metadata location from %s to %s "
                + "because it has been concurrently modified to %s",
            identifier, oldLocation, newLocation, existingLocation);
      }
      if (null == existingLocation) {
        createTableLike(identifier, entity);
      } else {
        updateTableLike(identifier, entity);
      }
    }

    @Override
    public FileIO io() {
      return viewFileIO;
    }

    @Override
    protected String viewName() {
      return fullViewName;
    }
  }

  private FileIO loadFileIOForTableLike(
      TableIdentifier identifier,
      Set<String> readLocations,
      PolarisResolvedPathWrapper resolvedStorageEntity,
      Map<String, String> tableProperties,
      Set<PolarisStorageActions> storageActions) {
    // Reload fileIO based on table specific context
    FileIO fileIO =
        fileIOFactory.loadFileIO(
            callContext,
            ioImplClassName,
            tableProperties,
            identifier,
            readLocations,
            storageActions,
            resolvedStorageEntity);
    // ensure the new fileIO is closed when the catalog is closed
    closeableGroup.addCloseable(fileIO);
    return fileIO;
  }

  private PolarisCallContext getCurrentPolarisContext() {
    return callContext.getPolarisCallContext();
  }

  private PolarisMetaStoreManager getMetaStoreManager() {
    return metaStoreManager;
  }

  private PolarisCredentialVendor getCredentialVendor() {
    return metaStoreManager;
  }

  @VisibleForTesting
  public void setFileIOFactory(FileIOFactory newFactory) {
    this.fileIOFactory = newFactory;
  }

  @VisibleForTesting
  long getCatalogId() {
    // TODO: Properly handle initialization
    if (catalogId <= 0) {
      throw new RuntimeException(
          "Failed to initialize catalogId before using catalog with name: " + catalogName);
    }
    return catalogId;
  }

  private void renameTableLike(
      PolarisEntitySubType subType, TableIdentifier from, TableIdentifier to) {
    LOGGER.debug("Renaming tableLike from {} to {}", from, to);
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(from, PolarisEntityType.TABLE_LIKE, subType);
    if (resolvedEntities == null) {
      if (subType == PolarisEntitySubType.ICEBERG_VIEW) {
        throw new NoSuchViewException("Cannot rename %s to %s. View does not exist", from, to);
      } else {
        throw new NoSuchTableException("Cannot rename %s to %s. Table does not exist", from, to);
      }
    }
    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    PolarisEntity leafEntity = resolvedEntities.getRawLeafEntity();
    final IcebergTableLikeEntity toEntity;
    List<PolarisEntity> newCatalogPath = null;
    if (!from.namespace().equals(to.namespace())) {
      PolarisResolvedPathWrapper resolvedNewParentEntities =
          resolvedEntityView.getResolvedPath(to.namespace());
      if (resolvedNewParentEntities == null) {
        throw new NoSuchNamespaceException(
            "Cannot rename %s to %s. Namespace does not exist: %s", from, to, to.namespace());
      }
      newCatalogPath = resolvedNewParentEntities.getRawFullPath();

      // the "to" table has a new parent and a new name / namespace path
      toEntity =
          new IcebergTableLikeEntity.Builder(IcebergTableLikeEntity.of(leafEntity))
              .setTableIdentifier(to)
              .setParentId(resolvedNewParentEntities.getResolvedLeafEntity().getEntity().getId())
              .build();
    } else {
      // only the name of the entity is changed
      toEntity =
          new IcebergTableLikeEntity.Builder(IcebergTableLikeEntity.of(leafEntity))
              .setTableIdentifier(to)
              .build();
    }

    // rename the entity now
    EntityResult returnedEntityResult =
        getMetaStoreManager()
            .renameEntity(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(catalogPath),
                leafEntity,
                PolarisEntity.toCoreList(newCatalogPath),
                toEntity);

    // handle error
    if (!returnedEntityResult.isSuccess()) {
      LOGGER.debug(
          "Rename error {} trying to rename {} to {}. Checking existing object.",
          returnedEntityResult.getReturnStatus(),
          from,
          to);
      switch (returnedEntityResult.getReturnStatus()) {
        case BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS:
          {
            PolarisEntitySubType existingEntitySubType =
                returnedEntityResult.getAlreadyExistsEntitySubType();
            if (existingEntitySubType == null) {
              // this code path is unexpected
              throw new AlreadyExistsException(
                  "Cannot rename %s to %s. Object already exists", from, to);
            } else if (existingEntitySubType == PolarisEntitySubType.ICEBERG_TABLE) {
              throw new AlreadyExistsException(
                  "Cannot rename %s to %s. Table already exists", from, to);
            } else if (existingEntitySubType == PolarisEntitySubType.ICEBERG_VIEW) {
              throw new AlreadyExistsException(
                  "Cannot rename %s to %s. View already exists", from, to);
            }
            throw new IllegalStateException(
                String.format("Unexpected entity type '%s'", existingEntitySubType));
          }

        case BaseResult.ReturnStatus.ENTITY_NOT_FOUND:
          throw new NotFoundException("Cannot rename %s to %s. %s does not exist", from, to, from);

        // this is temporary. Should throw a special error that will be caught and retried
        case BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED:
        case BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED:
          throw new RuntimeException("concurrent update detected, please retry");

        // some entities cannot be renamed
        case BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RENAMED:
          throw new BadRequestException("Cannot rename built-in object %s", leafEntity.getName());

        // some entities cannot be renamed
        default:
          throw new IllegalStateException(
              "Unknown error status " + returnedEntityResult.getReturnStatus());
      }
    } else {
      IcebergTableLikeEntity returnedEntity =
          IcebergTableLikeEntity.of(returnedEntityResult.getEntity());
      if (!toEntity.getTableIdentifier().equals(returnedEntity.getTableIdentifier())) {
        // As long as there are older deployments which don't support the atomic update of the
        // internalProperties during rename, we can log and then patch it up explicitly
        // in a best-effort way.
        LOGGER
            .atError()
            .addKeyValue("toEntity.getTableIdentifier()", toEntity.getTableIdentifier())
            .addKeyValue("returnedEntity.getTableIdentifier()", returnedEntity.getTableIdentifier())
            .log("Returned entity identifier doesn't match toEntity identifier");
        getMetaStoreManager()
            .updateEntityPropertiesIfNotChanged(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(newCatalogPath),
                new IcebergTableLikeEntity.Builder(returnedEntity).setTableIdentifier(to).build());
      }
    }
  }

  /**
   * Caller must fill in all entity fields except parentId, since the caller may not want to
   * duplicate the logic to try to resolve parentIds before constructing the proposed entity. This
   * method will fill in the parentId if needed upon resolution.
   */
  private void createTableLike(TableIdentifier identifier, PolarisEntity entity) {
    this.identifier = identifier;
    this.entity = entity;
    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(identifier.namespace());
    if (resolvedParent == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved parent for TableIdentifier '%s'", identifier));
    }

    createTableLike(identifier, entity, resolvedParent);
  }

  private void createTableLike(
      TableIdentifier identifier, PolarisEntity entity, PolarisResolvedPathWrapper resolvedParent) {
    // Make sure the metadata file is valid for our allowed locations.
    String metadataLocation = IcebergTableLikeEntity.of(entity).getMetadataLocation();
    validateLocationForTableLike(identifier, metadataLocation, resolvedParent);

    List<PolarisEntity> catalogPath = resolvedParent.getRawFullPath();

    if (entity.getParentId() <= 0) {
      // TODO: Validate catalogPath size is at least 1 for catalog entity?
      entity =
          new PolarisEntity.Builder(entity)
              .setParentId(resolvedParent.getRawLeafEntity().getId())
              .build();
    }
    entity =
        new PolarisEntity.Builder(entity).setCreateTimestamp(System.currentTimeMillis()).build();

    EntityResult res =
        getMetaStoreManager()
            .createEntityIfNotExists(
                getCurrentPolarisContext(), PolarisEntity.toCoreList(catalogPath), entity);
    if (!res.isSuccess()) {
      switch (res.getReturnStatus()) {
        case BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED:
          throw new NotFoundException("Parent path does not exist for %s", identifier);

        case BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS:
          throw new AlreadyExistsException(
              "Iceberg table, view, or generic table already exists: %s", identifier);
        default:
          throw new IllegalStateException(
              String.format(
                  "Unknown error status for identifier %s: %s with extraInfo: %s",
                  identifier, res.getReturnStatus(), res.getExtraInformation()));
      }
    }
    PolarisEntity resultEntity = PolarisEntity.of(res);
    LOGGER.debug("Created TableLike entity {} with TableIdentifier {}", resultEntity, identifier);
  }

  private void updateTableLike(TableIdentifier identifier, PolarisEntity entity) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(identifier, entity.getType(), entity.getSubType());
    if (resolvedEntities == null) {
      // Illegal state because the identifier should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved TableIdentifier '%s'", identifier));
    }

    // Make sure the metadata file is valid for our allowed locations.
    String metadataLocation = IcebergTableLikeEntity.of(entity).getMetadataLocation();
    validateLocationForTableLike(identifier, metadataLocation, resolvedEntities);

    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    EntityResult res =
        getMetaStoreManager()
            .updateEntityPropertiesIfNotChanged(
                getCurrentPolarisContext(), PolarisEntity.toCoreList(catalogPath), entity);
    if (!res.isSuccess()) {
      switch (res.getReturnStatus()) {
        case BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED:
          throw new NotFoundException("Parent path does not exist for %s", identifier);

        case BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED:
          throw new CommitFailedException(
              "Failed to commit Table or View %s because it was concurrently modified", identifier);

        default:
          throw new IllegalStateException(
              String.format(
                  "Unknown error status for identifier %s: %s with extraInfo: %s",
                  identifier, res.getReturnStatus(), res.getExtraInformation()));
      }
    }
    PolarisEntity resultEntity = PolarisEntity.of(res);
    LOGGER.debug("Updated TableLike entity {} with TableIdentifier {}", resultEntity, identifier);
  }

  @SuppressWarnings("FormatStringAnnotation")
  private @Nonnull DropEntityResult dropTableLike(
      PolarisEntitySubType subType,
      TableIdentifier identifier,
      Map<String, String> storageProperties,
      boolean purge) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(identifier, PolarisEntityType.TABLE_LIKE, subType);
    if (resolvedEntities == null) {
      // TODO: Error?
      return new DropEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    PolarisEntity leafEntity = resolvedEntities.getRawLeafEntity();

    // Check that purge is enabled, if it is set:
    if (catalogPath != null && !catalogPath.isEmpty() && purge) {
      boolean dropWithPurgeEnabled =
          callContext
              .getPolarisCallContext()
              .getConfigurationStore()
              .getConfiguration(
                  callContext.getPolarisCallContext(),
                  catalogEntity,
                  FeatureConfiguration.DROP_WITH_PURGE_ENABLED);
      if (!dropWithPurgeEnabled) {
        throw new ForbiddenException(
            String.format(
                "Unable to purge entity: %s. To enable this feature, set the Polaris configuration %s "
                    + "or the catalog configuration %s",
                identifier.name(),
                FeatureConfiguration.DROP_WITH_PURGE_ENABLED.key,
                FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig()));
      }
    }

    return getMetaStoreManager()
        .dropEntityIfExists(
            getCurrentPolarisContext(),
            PolarisEntity.toCoreList(catalogPath),
            leafEntity,
            storageProperties,
            purge);
  }

  private boolean sendNotificationForTableLike(
      PolarisEntitySubType subType, TableIdentifier tableIdentifier, NotificationRequest request) {
    LOGGER.debug(
        "Handling notification request {} for tableIdentifier {}", request, tableIdentifier);
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            tableIdentifier, PolarisEntityType.TABLE_LIKE, subType);

    NotificationType notificationType = request.getNotificationType();

    Preconditions.checkNotNull(notificationType, "Expected a valid notification type.");

    if (notificationType == NotificationType.DROP) {
      return dropTableLike(
              PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, Map.of(), false /* purge */)
          .isSuccess();
    } else if (notificationType == NotificationType.VALIDATE) {
      // In this mode we don't want to make any mutations, so we won't auto-create non-existing
      // parent namespaces. This means when we want to validate allowedLocations for the proposed
      // table metadata location, we must independently find the deepest non-null parent namespace
      // of the TableIdentifier, which may even be the base CatalogEntity if no parent namespaces
      // actually exist yet. We can then extract the right StorageInfo entity via a normal call
      // to findStorageInfoFromHierarchy.
      PolarisResolvedPathWrapper resolvedStorageEntity = null;
      Optional<PolarisEntity> storageInfoEntity = Optional.empty();
      for (int i = tableIdentifier.namespace().length(); i >= 0; i--) {
        Namespace nsLevel =
            Namespace.of(
                Arrays.stream(tableIdentifier.namespace().levels())
                    .limit(i)
                    .toArray(String[]::new));
        resolvedStorageEntity = resolvedEntityView.getResolvedPath(nsLevel);
        if (resolvedStorageEntity != null) {
          storageInfoEntity = FileIOUtil.findStorageInfoFromHierarchy(resolvedStorageEntity);
          break;
        }
      }

      if (resolvedStorageEntity == null || storageInfoEntity.isEmpty()) {
        throw new BadRequestException(
            "Failed to find StorageInfo entity for TableIdentifier %s", tableIdentifier);
      }

      // Validate location against the resolvedStorageEntity
      String metadataLocation =
          transformTableLikeLocation(request.getPayload().getMetadataLocation());
      validateLocationForTableLike(tableIdentifier, metadataLocation, resolvedStorageEntity);

      // Validate that we can construct a FileIO
      String locationDir = metadataLocation.substring(0, metadataLocation.lastIndexOf("/"));
      loadFileIOForTableLike(
          tableIdentifier,
          Set.of(locationDir),
          resolvedStorageEntity,
          new HashMap<>(tableDefaultProperties),
          Set.of(PolarisStorageActions.READ));

      LOGGER.debug(
          "Successful VALIDATE notification for tableIdentifier {}, metadataLocation {}",
          tableIdentifier,
          metadataLocation);
    } else if (notificationType == NotificationType.CREATE
        || notificationType == NotificationType.UPDATE) {

      Namespace ns = tableIdentifier.namespace();
      createNonExistingNamespaces(ns);

      PolarisResolvedPathWrapper resolvedParent = resolvedEntityView.getPassthroughResolvedPath(ns);

      IcebergTableLikeEntity entity =
          IcebergTableLikeEntity.of(
              resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());

      String existingLocation;
      String newLocation = transformTableLikeLocation(request.getPayload().getMetadataLocation());
      if (null == entity) {
        existingLocation = null;
        entity =
            new IcebergTableLikeEntity.Builder(tableIdentifier, newLocation)
                .setCatalogId(getCatalogId())
                .setSubType(PolarisEntitySubType.ICEBERG_TABLE)
                .setId(
                    getMetaStoreManager().generateNewEntityId(getCurrentPolarisContext()).getId())
                .setLastNotificationTimestamp(request.getPayload().getTimestamp())
                .build();
      } else {
        // If the notification timestamp is out-of-order, we should not update the table
        if (entity.getLastAdmittedNotificationTimestamp().isPresent()
            && request.getPayload().getTimestamp()
                <= entity.getLastAdmittedNotificationTimestamp().get()) {
          throw new AlreadyExistsException(
              "A notification with a newer timestamp has been processed for table %s",
              tableIdentifier);
        }
        existingLocation = entity.getMetadataLocation();
        entity =
            new IcebergTableLikeEntity.Builder(entity)
                .setMetadataLocation(newLocation)
                .setLastNotificationTimestamp(request.getPayload().getTimestamp())
                .build();
      }
      // first validate we can read the metadata file
      validateLocationForTableLike(tableIdentifier, newLocation);

      String locationDir = newLocation.substring(0, newLocation.lastIndexOf("/"));

      FileIO fileIO =
          loadFileIOForTableLike(
              tableIdentifier,
              Set.of(locationDir),
              resolvedParent,
              new HashMap<>(tableDefaultProperties),
              Set.of(PolarisStorageActions.READ, PolarisStorageActions.WRITE));
      TableMetadata tableMetadata = TableMetadataParser.read(fileIO, newLocation);

      // then validate that it points to a valid location for this table
      validateLocationForTableLike(tableIdentifier, tableMetadata.location());

      // finally, validate that the metadata file is within the table directory
      validateMetadataFileInTableDir(
          tableIdentifier,
          tableMetadata,
          CatalogEntity.of(resolvedParent.getRawFullPath().getFirst()));

      // TODO: These might fail due to concurrent update; we need to do a retry in those cases.
      if (null == existingLocation) {
        LOGGER.debug(
            "Creating table {} for notification with metadataLocation {}",
            tableIdentifier,
            newLocation);
        createTableLike(tableIdentifier, entity, resolvedParent);
      } else {
        LOGGER.debug(
            "Updating table {} for notification with metadataLocation {}",
            tableIdentifier,
            newLocation);

        updateTableLike(tableIdentifier, entity);
      }
    }
    return true;
  }

  private void createNonExistingNamespaces(Namespace namespace) {
    // Pre-create namespaces if they don't exist
    for (int i = 1; i <= namespace.length(); i++) {
      Namespace nsLevel =
          Namespace.of(Arrays.stream(namespace.levels()).limit(i).toArray(String[]::new));
      if (resolvedEntityView.getPassthroughResolvedPath(nsLevel) == null) {
        Namespace parentNamespace = PolarisCatalogHelpers.getParentNamespace(nsLevel);
        PolarisResolvedPathWrapper resolvedParent =
            resolvedEntityView.getPassthroughResolvedPath(parentNamespace);
        createNamespaceInternal(nsLevel, Collections.emptyMap(), resolvedParent);
      }
    }
  }

  private List<TableIdentifier> listTableLike(PolarisEntitySubType subType, Namespace namespace) {
    PolarisResolvedPathWrapper resolvedEntities = resolvedEntityView.getResolvedPath(namespace);
    if (resolvedEntities == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved namespace '%s'", namespace));
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawFullPath();
    List<PolarisEntity.NameAndId> entities =
        PolarisEntity.toNameAndIdList(
            getMetaStoreManager()
                .listEntities(
                    getCurrentPolarisContext(),
                    PolarisEntity.toCoreList(catalogPath),
                    PolarisEntityType.TABLE_LIKE,
                    subType)
                .getEntities());
    return PolarisCatalogHelpers.nameAndIdToTableIdentifiers(catalogPath, entities);
  }

  /**
   * Load FileIO with provided impl and properties
   *
   * @param ioImpl full class name of a custom FileIO implementation
   * @param properties used to initialize the FileIO implementation
   * @return FileIO object
   */
  private FileIO loadFileIO(String ioImpl, Map<String, String> properties) {
    IcebergTableLikeEntity icebergTableLikeEntity = IcebergTableLikeEntity.of(catalogEntity);
    TableIdentifier identifier = icebergTableLikeEntity.getTableIdentifier();
    Set<String> locations = Set.of(catalogEntity.getDefaultBaseLocation());
    ResolvedPolarisEntity resolvedCatalogEntity =
        new ResolvedPolarisEntity(catalogEntity, List.of(), List.of());
    PolarisResolvedPathWrapper resolvedPath =
        new PolarisResolvedPathWrapper(List.of(resolvedCatalogEntity));
    Set<PolarisStorageActions> storageActions = Set.of(PolarisStorageActions.ALL);
    return fileIOFactory.loadFileIO(
        callContext, ioImpl, properties, identifier, locations, storageActions, resolvedPath);
  }

  private void blockedUserSpecifiedWriteLocation(Map<String, String> properties) {
    if (properties != null
        && (properties.containsKey(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY)
            || properties.containsKey(
                IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY))) {
      throw new ForbiddenException(
          "Delegate access to table with user-specified write location is temporarily not supported.");
    }
  }

  /** Helper to retrieve dynamic context-based configuration that has a boolean value. */
  private Boolean getBooleanContextConfiguration(String configKey, boolean defaultValue) {
    return callContext
        .getPolarisCallContext()
        .getConfigurationStore()
        .getConfiguration(callContext.getPolarisCallContext(), configKey, defaultValue);
  }

  private int getMaxMetadataRefreshRetries() {
    return callContext
        .getPolarisCallContext()
        .getConfigurationStore()
        .getConfiguration(
            callContext.getPolarisCallContext(), FeatureConfiguration.MAX_METADATA_REFRESH_RETRIES);
  }
}
