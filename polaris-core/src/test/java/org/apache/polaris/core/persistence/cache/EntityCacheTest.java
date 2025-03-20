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
package org.apache.polaris.core.persistence.cache;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.TableLikeEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.transactional.PolarisMetaStoreManagerImpl;
import org.apache.polaris.core.persistence.transactional.PolarisTreeMapMetaStoreSessionImpl;
import org.apache.polaris.core.persistence.transactional.PolarisTreeMapStore;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Unit testing of the entity cache */
public class EntityCacheTest {

  // diag services
  private final PolarisDiagnostics diagServices;

  // the entity store, use treemap implementation
  private final PolarisTreeMapStore store;

  // to interact with the metastore
  private final TransactionalPersistence metaStore;

  // polaris call context
  private final PolarisCallContext callCtx;

  // utility to bootstrap the mata store
  private final PolarisTestMetaStoreManager tm;

  // the meta store manager
  private final PolarisMetaStoreManager metaStoreManager;

  /**
   * Initialize and create the test metadata
   *
   * <pre>
   * - test
   * - (N1/N2/T1)
   * - (N1/N2/T2)
   * - (N1/N2/V1)
   * - (N1/N3/T3)
   * - (N1/N3/V2)
   * - (N1/T4)
   * - (N1/N4)
   * - N5/N6/T5
   * - N5/N6/T6
   * - R1(TABLE_READ on N1/N2, VIEW_CREATE on C, TABLE_LIST on N2, TABLE_DROP on N5/N6/T5)
   * - R2(TABLE_WRITE_DATA on N5, VIEW_LIST on C)
   * - PR1(R1, R2)
   * - PR2(R2)
   * - P1(PR1, PR2)
   * - P2(PR2)
   * </pre>
   */
  public EntityCacheTest() {
    diagServices = new PolarisDefaultDiagServiceImpl();
    store = new PolarisTreeMapStore(diagServices);
    metaStore = new PolarisTreeMapMetaStoreSessionImpl(store, Mockito.mock(), RANDOM_SECRETS);
    callCtx = new PolarisCallContext(metaStore, diagServices);
    metaStoreManager = new PolarisMetaStoreManagerImpl();

    // bootstrap the mata store with our test schema
    tm = new PolarisTestMetaStoreManager(metaStoreManager, callCtx);
    tm.testCreateTestCatalog();
  }

  /**
   * @return new cache for the entity store
   */
  EntityCache allocateNewCache(int multiplier) {
    return new EntityCache(this.metaStoreManager, multiplier);
  }

  private static final String ASCII_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  private static final Random random = new Random();

  public static String randomString(int length, boolean asciiOnly) {
    StringBuilder sb = new StringBuilder(length);
    if (asciiOnly) {
      for (int i = 0; i < length; i++) {
        sb.append(ASCII_CHARS.charAt(random.nextInt(ASCII_CHARS.length())));
      }
    } else {
      for (int i = 0; i < length; i++) {
        sb.append((char) (random.nextInt(0x7FFF - 0x1000) + 0x1000));
      }
    }
    return sb.toString();
  }

  long getHeapSize() {
    System.gc();
    System.runFinalization();
    return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
  }

  private ResolvedPolarisEntity buildResolvedEntity(int characters, boolean useAscii) {
    int nameSize = 10;
    PolarisBaseEntity entity =
        new PolarisBaseEntity(
            1,
            random.nextLong(),
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.ANY_SUBTYPE,
            1,
            randomString(nameSize, useAscii));
    entity.setProperties(randomString(characters - nameSize, useAscii));
    return new ResolvedPolarisEntity(callCtx.getDiagServices(), entity, List.of(), 1);
  }

  @Test
  void testHeapSize() throws InterruptedException {
    long targetCharactersToWrite = 1L * 1000 * 1000 * 1000;
    int printInterval = 1000;
    int trials = 1;
    boolean[] asciiProperties = {true};
    int[] propertyCharacters = {1000};
    int[] multipliers = {1, 2, 3, 4, 5, 6, 7, 8};

    EntityCache cache = allocateNewCache(1);
    for (int trial = 0; trial < trials; trial++) {
      for (boolean useAscii : asciiProperties) {
        for (int propertyLength : propertyCharacters) {
          for (int multiplier : multipliers) {
            cache.cleanup();
            cache = allocateNewCache(multiplier);
            System.gc();
            System.runFinalization();
            Thread.sleep(2000);
            long baselineHeapSize = getHeapSize();

            for (int i = 0; i < targetCharactersToWrite / (propertyLength + 100); i++) {
              cache.cacheNewEntry(buildResolvedEntity(propertyLength, useAscii));

              if (i % printInterval == 0) {
                System.out.printf(
                    "%d,%s,%d,%d,%d,%d\n",
                    trial,
                    useAscii,
                    propertyLength,
                    i,
                    getHeapSize() - baselineHeapSize,
                    cache.estimatedSize());
              }
            }
          }
        }
      }
    }
  }
}
