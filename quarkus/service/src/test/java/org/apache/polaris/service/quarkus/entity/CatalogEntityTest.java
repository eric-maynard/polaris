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
package org.apache.polaris.service.quarkus.entity;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class CatalogEntityTest {

  @BeforeAll
  public static void setup() {
    MetaStoreManagerFactory metaStoreManagerFactory = new InMemoryPolarisMetaStoreManagerFactory();
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            metaStoreManagerFactory.getOrCreateSessionSupplier(() -> "realm").get(),
            new PolarisDefaultDiagServiceImpl());
    CallContext callContext = CallContext.of(() -> "realm", polarisCallContext);
    CallContext.setCurrentContext(callContext);
  }

  @Test
  public void testInvalidAllowedLocationPrefix() {
    String storageLocation = "unsupportPrefix://mybucket/path";
    AwsStorageConfigInfo awsStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(storageLocation, "s3://externally-owned-bucket"))
            .build();
    CatalogProperties prop = new CatalogProperties(storageLocation);
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(prop)
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(awsCatalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Location prefix not allowed: 'unsupportPrefix://mybucket/path', expected prefixes");

    // Invalid azure prefix
    AzureStorageConfigInfo azureStorageConfigModel =
        AzureStorageConfigInfo.builder()
            .setAllowedLocations(
                List.of(storageLocation, "abfs://container@storageaccount.blob.windows.net/path"))
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setTenantId("tenantId")
            .build();
    Catalog azureCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(
                new CatalogProperties("abfs://container@storageaccount.blob.windows.net/path"))
            .setStorageConfigInfo(azureStorageConfigModel)
            .build();
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(azureCatalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid azure location uri unsupportPrefix://mybucket/path");

    // invalid gcp prefix
    GcpStorageConfigInfo gcpStorageConfigModel =
        GcpStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setAllowedLocations(List.of(storageLocation, "gs://externally-owned-bucket"))
            .build();
    Catalog gcpCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(new CatalogProperties("gs://externally-owned-bucket"))
            .setStorageConfigInfo(gcpStorageConfigModel)
            .build();
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(gcpCatalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Location prefix not allowed: 'unsupportPrefix://mybucket/path', expected prefixes");
  }

  @Test
  public void testExceedMaxAllowedLocations() {
    String storageLocation = "s3://mybucket/path/";
    AwsStorageConfigInfo awsStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(
                List.of(
                    storageLocation + "1/",
                    storageLocation + "2/",
                    storageLocation + "3/",
                    storageLocation + "4/",
                    storageLocation + "5/",
                    storageLocation + "6/"))
            .build();
    CatalogProperties prop = new CatalogProperties(storageLocation);
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(prop)
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();
    Assertions.assertThatCode(() -> CatalogEntity.fromCatalog(awsCatalog)).doesNotThrowAnyException();
  }

  @Test
  public void testValidAllowedLocationPrefix() {
    String basedLocation = "s3://externally-owned-bucket";
    AwsStorageConfigInfo awsStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(basedLocation))
            .build();

    CatalogProperties prop = new CatalogProperties(basedLocation);
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(prop)
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();
    Assertions.assertThatNoException().isThrownBy(() -> CatalogEntity.fromCatalog(awsCatalog));

    basedLocation = "abfs://container@storageaccount.blob.windows.net/path";
    prop.put(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, basedLocation);
    AzureStorageConfigInfo azureStorageConfigModel =
        AzureStorageConfigInfo.builder()
            .setAllowedLocations(List.of(basedLocation))
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setTenantId("tenantId")
            .build();
    Catalog azureCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(new CatalogProperties(basedLocation))
            .setStorageConfigInfo(azureStorageConfigModel)
            .build();
    Assertions.assertThatNoException().isThrownBy(() -> CatalogEntity.fromCatalog(azureCatalog));

    basedLocation = "gs://externally-owned-bucket";
    prop.put(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, basedLocation);
    GcpStorageConfigInfo gcpStorageConfigModel =
        GcpStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setAllowedLocations(List.of(basedLocation))
            .build();
    Catalog gcpCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(new CatalogProperties(basedLocation))
            .setStorageConfigInfo(gcpStorageConfigModel)
            .build();
    Assertions.assertThatNoException().isThrownBy(() -> CatalogEntity.fromCatalog(gcpCatalog));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "arn:aws:iam::0123456:role/jdoe", "aws-cn"})
  public void testInvalidArn(String roleArn) {
    String basedLocation = "s3://externally-owned-bucket";
    AwsStorageConfigInfo awsStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn(roleArn)
            .setExternalId("externalId")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(basedLocation))
            .build();

    CatalogProperties prop = new CatalogProperties(basedLocation);
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(prop)
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();
    String expectedMessage = "";
    switch (roleArn) {
      case "":
        expectedMessage = "ARN cannot be null or empty";
        break;
      case "aws-cn":
        expectedMessage = "AWS China is temporarily not supported";
        break;
      default:
        expectedMessage = "Invalid role ARN format";
    }
    ;
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(awsCatalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }
}
