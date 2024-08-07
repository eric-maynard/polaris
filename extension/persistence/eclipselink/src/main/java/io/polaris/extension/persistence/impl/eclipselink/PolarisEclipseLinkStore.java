/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.extension.persistence.impl.eclipselink;

import io.polaris.core.PolarisDiagnostics;
import io.polaris.core.entity.PolarisBaseEntity;
import io.polaris.core.entity.PolarisEntitiesActiveKey;
import io.polaris.core.entity.PolarisEntityActiveRecord;
import io.polaris.core.entity.PolarisEntityCore;
import io.polaris.core.entity.PolarisEntityId;
import io.polaris.core.entity.PolarisEntityType;
import io.polaris.core.entity.PolarisGrantRecord;
import io.polaris.core.entity.PolarisPrincipalSecrets;
import io.polaris.core.persistence.models.ModelEntity;
import io.polaris.core.persistence.models.ModelEntityActive;
import io.polaris.core.persistence.models.ModelEntityChangeTracking;
import io.polaris.core.persistence.models.ModelEntityDropped;
import io.polaris.core.persistence.models.ModelGrantRecord;
import io.polaris.core.persistence.models.ModelPrincipalSecrets;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.persistence.TypedQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements an EclipseLink based metastore for Polaris which can be configured for any database
 * with EclipseLink support
 */
public class PolarisEclipseLinkStore {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisEclipseLinkStore.class);
  private static final String TIMEOUT_PARAMETER = "jakarta.persistence.query.timeout";

  // diagnostic services
  private final PolarisDiagnostics diagnosticServices;

  /**
   * Constructor, allocate everything at once
   *
   * @param diagnostics diagnostic services
   */
  public PolarisEclipseLinkStore(@NotNull PolarisDiagnostics diagnostics) {
    this.diagnosticServices = diagnostics;
  }

  private Optional<Long> getTimeoutMs(EntityManager entityManager) {
    Map<String, Object> properties = entityManager.getEntityManagerFactory().getProperties();
    if (properties.containsKey(TIMEOUT_PARAMETER)) {
      return Optional.of(Long.parseLong(String.valueOf(properties.get(TIMEOUT_PARAMETER))));
    }
    return Optional.empty();
  }

  private Query createQuery(EntityManager entityManager, String queryString) {
    Query query = entityManager
        .createQuery(queryString);
    Optional<Long> timeout = getTimeoutMs(entityManager);
    if (timeout.isPresent()) {
      query.setHint(TIMEOUT_PARAMETER, timeout.get());
    }
    return query;
  }

  private <T> TypedQuery<T> createQuery(EntityManager entityManager, String queryString, Class<T> typ) {
    TypedQuery<T> query = entityManager
        .createQuery(queryString, typ);
    Optional<Long> timeout = getTimeoutMs(entityManager);
    if (timeout.isPresent()) {
      query.setHint(TIMEOUT_PARAMETER, timeout.get());
    }
    return query;
  }

  long getNextSequence(EntityManager session) {
    diagnosticServices.check(session != null, "session_is_null");
    // implement with a sequence table POLARIS_SEQUENCE
    return (long) session.createNativeQuery("SELECT NEXTVAL('POLARIS_SEQ')").getSingleResult();
  }

  void writeToEntities(EntityManager session, PolarisBaseEntity entity) {
    diagnosticServices.check(session != null, "session_is_null");

    ModelEntity model = lookupEntity(session, entity.getCatalogId(), entity.getId());
    if (model != null) {
      // Update if the same entity already exists
      model.update(entity);
    } else {
      model = ModelEntity.fromEntity(entity);
    }

    session.persist(model);
  }

  void writeToEntitiesActive(EntityManager session, PolarisBaseEntity entity) {
    diagnosticServices.check(session != null, "session_is_null");

    ModelEntityActive model = lookupEntityActive(session, new PolarisEntitiesActiveKey(entity));
    if (model == null) {
      session.persist(ModelEntityActive.fromEntityActive(new PolarisEntityActiveRecord(entity)));
    }
  }

  void writeToEntitiesDropped(EntityManager session, PolarisBaseEntity entity) {
    diagnosticServices.check(session != null, "session_is_null");

    ModelEntityDropped entityDropped =
        lookupEntityDropped(session, entity.getCatalogId(), entity.getId());
    if (entityDropped == null) {
      session.persist(ModelEntityDropped.fromEntity(entity));
    }
  }

  void writeToEntitiesChangeTracking(EntityManager session, PolarisBaseEntity entity) {
    diagnosticServices.check(session != null, "session_is_null");

    // Update the existing change tracking if a record with the same ids exists; otherwise, persist
    // a new one
    ModelEntityChangeTracking entityChangeTracking =
        lookupEntityChangeTracking(session, entity.getCatalogId(), entity.getId());
    if (entityChangeTracking != null) {
      entityChangeTracking.update(entity);
    } else {
      entityChangeTracking = new ModelEntityChangeTracking(entity);
    }

    session.persist(entityChangeTracking);
  }

  void writeToGrantRecords(EntityManager session, PolarisGrantRecord grantRec) {
    diagnosticServices.check(session != null, "session_is_null");

    session.persist(ModelGrantRecord.fromGrantRecord(grantRec));
  }

  void deleteFromEntities(EntityManager session, long catalogId, long entityId) {
    diagnosticServices.check(session != null, "session_is_null");

    ModelEntity model = lookupEntity(session, catalogId, entityId);
    diagnosticServices.check(model != null, "entity_not_found");

    session.remove(model);
  }

  void deleteFromEntitiesActive(EntityManager session, PolarisEntitiesActiveKey key) {
    diagnosticServices.check(session != null, "session_is_null");

    ModelEntityActive entity = lookupEntityActive(session, key);
    diagnosticServices.check(entity != null, "active_entity_not_found");
    session.remove(entity);
  }

  void deleteFromEntitiesDropped(EntityManager session, long catalogId, long entityId) {
    diagnosticServices.check(session != null, "session_is_null");

    ModelEntityDropped entity = lookupEntityDropped(session, catalogId, entityId);
    diagnosticServices.check(entity != null, "dropped_entity_not_found");

    session.remove(entity);
  }

  void deleteFromEntitiesChangeTracking(EntityManager session, PolarisEntityCore entity) {
    diagnosticServices.check(session != null, "session_is_null");

    ModelEntityChangeTracking entityChangeTracking =
        lookupEntityChangeTracking(session, entity.getCatalogId(), entity.getId());
    diagnosticServices.check(entityChangeTracking != null, "change_tracking_entity_not_found");

    session.remove(entityChangeTracking);
  }

  void deleteFromGrantRecords(EntityManager session, PolarisGrantRecord grantRec) {
    diagnosticServices.check(session != null, "session_is_null");

    ModelGrantRecord lookupGrantRecord =
        lookupGrantRecord(
            session,
            grantRec.getSecurableCatalogId(),
            grantRec.getSecurableId(),
            grantRec.getGranteeCatalogId(),
            grantRec.getGranteeId(),
            grantRec.getPrivilegeCode());

    diagnosticServices.check(lookupGrantRecord != null, "grant_record_not_found");

    session.remove(lookupGrantRecord);
  }

  void deleteAllEntityGrantRecords(EntityManager session, PolarisEntityCore entity) {
    diagnosticServices.check(session != null, "session_is_null");

    // Delete grant records from grantRecords tables
    lookupAllGrantRecordsOnSecurable(session, entity.getCatalogId(), entity.getId())
        .forEach(session::remove);

    // Delete grantee records from grantRecords tables
    lookupGrantRecordsOnGrantee(session, entity.getCatalogId(), entity.getId())
        .forEach(session::remove);
  }

  void deleteAll(EntityManager session) {
    diagnosticServices.check(session != null, "session_is_null");

    createQuery(session, "DELETE from ModelEntity").executeUpdate();
    createQuery(session, "DELETE from ModelEntityActive").executeUpdate();
    createQuery(session, "DELETE from ModelEntityDropped").executeUpdate();
    createQuery(session, "DELETE from ModelEntityChangeTracking").executeUpdate();
    createQuery(session, "DELETE from ModelGrantRecord").executeUpdate();
    createQuery(session, "DELETE from ModelPrincipalSecrets").executeUpdate();

    LOG.debug("All entities deleted.");
  }

  ModelEntity lookupEntity(EntityManager session, long catalogId, long entityId) {
    diagnosticServices.check(session != null, "session_is_null");

    return createQuery(
            session,
            "SELECT m from ModelEntity m where m.catalogId=:catalogId and m.id=:id",
            ModelEntity.class)
        .setParameter("catalogId", catalogId)
        .setParameter("id", entityId)
        .getResultStream()
        .findFirst()
        .orElse(null);
  }

  @SuppressWarnings("unchecked")
  List<ModelEntity> lookupEntities(EntityManager session, List<PolarisEntityId> entityIds) {
    diagnosticServices.check(session != null, "session_is_null");

    if (entityIds == null || entityIds.isEmpty()) return new ArrayList<>();

    // TODO Support paging
    String inClause =
        entityIds.stream()
            .map(entityId -> "(" + entityId.getCatalogId() + "," + entityId.getId() + ")")
            .collect(Collectors.joining(","));

    String hql = "SELECT * from ENTITIES m where (m.catalogId, m.id) in (" + inClause + ")";
    Query query = session.createNativeQuery(hql, ModelEntity.class);
    if (getTimeoutMs(session).isPresent()) {
      query.setHint(TIMEOUT_PARAMETER, getTimeoutMs(session).get());
    }
    return (List<ModelEntity>) query.getResultList();
  }

  ModelEntityActive lookupEntityActive(
      EntityManager session, PolarisEntitiesActiveKey entityActiveKey) {
    diagnosticServices.check(session != null, "session_is_null");

    return createQuery(
            session,
            "SELECT m from ModelEntityActive m where m.catalogId=:catalogId and m.parentId=:parentId and m.typeCode=:typeCode and m.name=:name",
            ModelEntityActive.class)
        .setParameter("catalogId", entityActiveKey.getCatalogId())
        .setParameter("parentId", entityActiveKey.getParentId())
        .setParameter("typeCode", entityActiveKey.getTypeCode())
        .setParameter("name", entityActiveKey.getName())
        .getResultStream()
        .findFirst()
        .orElse(null);
  }

  long countActiveChildEntities(
      EntityManager session,
      long catalogId,
      long parentId,
      @Nullable PolarisEntityType entityType) {
    diagnosticServices.check(session != null, "session_is_null");

    String hql =
        "SELECT COUNT(m) from ModelEntityActive m where m.catalogId=:catalogId and m.parentId=:parentId";
    if (entityType != null) {
      hql += " and m.typeCode=:typeCode";
    }

    TypedQuery<Long> query =
        createQuery(
          session,
          hql,
          Long.class)
            .setParameter("catalogId", catalogId)
            .setParameter("parentId", parentId);
    if (entityType != null) {
      query.setParameter("typeCode", entityType.getCode());
    }

    return query.getSingleResult();
  }

  List<ModelEntity> lookupFullEntitiesActive(
      EntityManager session, long catalogId, long parentId, @NotNull PolarisEntityType entityType) {
    diagnosticServices.check(session != null, "session_is_null");

    // Currently check against ENTITIES not joining with ENTITIES_ACTIVE
    String hql =
        "SELECT m from ModelEntity m where m.catalogId=:catalogId and m.parentId=:parentId and m.typeCode=:typeCode";

    TypedQuery<ModelEntity> query =
        createQuery(
            session,
            hql,
            ModelEntity.class)
            .setParameter("catalogId", catalogId)
            .setParameter("parentId", parentId)
            .setParameter("typeCode", entityType.getCode());

    return query.getResultList();
  }

  ModelEntityDropped lookupEntityDropped(EntityManager session, long catalogId, long entityId) {
    diagnosticServices.check(session != null, "session_is_null");

    return createQuery(
            session,
            "SELECT m from ModelEntityDropped m where m.catalogId=:catalogId and m.id=:id",
            ModelEntityDropped.class)
        .setParameter("catalogId", catalogId)
        .setParameter("id", entityId)
        .getResultStream()
        .findFirst()
        .orElse(null);
  }

  ModelEntityChangeTracking lookupEntityChangeTracking(
      EntityManager session, long catalogId, long entityId) {
    diagnosticServices.check(session != null, "session_is_null");

    return createQuery(
            session,
            "SELECT m from ModelEntityChangeTracking m where m.catalogId=:catalogId and m.id=:id",
            ModelEntityChangeTracking.class)
        .setParameter("catalogId", catalogId)
        .setParameter("id", entityId)
        .getResultStream()
        .findFirst()
        .orElse(null);
  }

  ModelGrantRecord lookupGrantRecord(
      EntityManager session,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    diagnosticServices.check(session != null, "session_is_null");

    return createQuery(
            session,
            "SELECT m from ModelGrantRecord m where m.securableCatalogId=:securableCatalogId "
                + "and m.securableId=:securableId "
                + "and m.granteeCatalogId=:granteeCatalogId "
                + "and m.granteeId=:granteeId "
                + "and m.privilegeCode=:privilegeCode",
            ModelGrantRecord.class)
        .setParameter("securableCatalogId", securableCatalogId)
        .setParameter("securableId", securableId)
        .setParameter("granteeCatalogId", granteeCatalogId)
        .setParameter("granteeId", granteeId)
        .setParameter("privilegeCode", privilegeCode)
        .getResultStream()
        .findFirst()
        .orElse(null);
  }

  List<ModelGrantRecord> lookupAllGrantRecordsOnSecurable(
      EntityManager session, long securableCatalogId, long securableId) {
    diagnosticServices.check(session != null, "session_is_null");

    return createQuery(
            session,
            "SELECT m from ModelGrantRecord m "
                + "where m.securableCatalogId=:securableCatalogId "
                + "and m.securableId=:securableId",
            ModelGrantRecord.class)
        .setParameter("securableCatalogId", securableCatalogId)
        .setParameter("securableId", securableId)
        .getResultList();
  }

  List<ModelGrantRecord> lookupGrantRecordsOnGrantee(
      EntityManager session, long granteeCatalogId, long granteeId) {
    diagnosticServices.check(session != null, "session_is_null");

    return createQuery(
            session,
            "SELECT m from ModelGrantRecord m "
                + "where m.granteeCatalogId=:granteeCatalogId "
                + "and m.granteeId=:granteeId",
            ModelGrantRecord.class)
        .setParameter("granteeCatalogId", granteeCatalogId)
        .setParameter("granteeId", granteeId)
        .getResultList();
  }

  ModelPrincipalSecrets lookupPrincipalSecrets(EntityManager session, String clientId) {
    diagnosticServices.check(session != null, "session_is_null");

    return createQuery(
            session,
            "SELECT m from ModelPrincipalSecrets m where m.principalClientId=:clientId",
            ModelPrincipalSecrets.class)
        .setParameter("clientId", clientId)
        .getResultStream()
        .findFirst()
        .orElse(null);
  }

  void writePrincipalSecrets(EntityManager session, PolarisPrincipalSecrets principalSecrets) {
    diagnosticServices.check(session != null, "session_is_null");

    ModelPrincipalSecrets modelPrincipalSecrets =
        lookupPrincipalSecrets(session, principalSecrets.getPrincipalClientId());
    if (modelPrincipalSecrets != null) {
      modelPrincipalSecrets.update(principalSecrets);
    } else {
      modelPrincipalSecrets = ModelPrincipalSecrets.fromPrincipalSecrets(principalSecrets);
    }

    session.persist(modelPrincipalSecrets);
  }

  void deletePrincipalSecrets(EntityManager session, String clientId) {
    diagnosticServices.check(session != null, "session_is_null");

    ModelPrincipalSecrets modelPrincipalSecrets = lookupPrincipalSecrets(session, clientId);
    diagnosticServices.check(modelPrincipalSecrets != null, "principal_secretes_not_found");

    session.remove(modelPrincipalSecrets);
  }
}
