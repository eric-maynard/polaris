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
package org.apache.polaris.service.storage.azure;

import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.azure.AzureLocation;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class AzureLocationTest {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void testLocation() {
    String uri = "abfss://container@storageaccount.blob.core.windows.net/myfile";
    StorageLocation storageLocation = StorageLocation.of(uri);
    AzureLocation azureLocation = (AzureLocation) storageLocation;
    soft.assertThat(azureLocation.getContainer()).isEqualTo("container");
    soft.assertThat(azureLocation.getStorageAccount()).isEqualTo("storageaccount");
    soft.assertThat(azureLocation.getEndpoint()).isEqualTo("blob.core.windows.net");
    soft.assertThat(azureLocation.getFilePath()).isEqualTo("myfile");
  }

  @Test
  public void testWasbLocation() {
    String uri = "wasb://container@storageaccount.blob.core.windows.net/myfile";
    StorageLocation storageLocation = StorageLocation.of(uri);
    AzureLocation azureLocation = (AzureLocation) storageLocation;
    soft.assertThat(azureLocation.getContainer()).isEqualTo("container");
    soft.assertThat(azureLocation.getStorageAccount()).isEqualTo("storageaccount");
    soft.assertThat(azureLocation.getEndpoint()).isEqualTo("blob.core.windows.net");
    soft.assertThat(azureLocation.getFilePath()).isEqualTo("myfile");
  }

  @Test
  public void testCrossSchemeComparisons() {
    StorageLocation abfsLocation =
        StorageLocation.of("abfss://container@acc.dev.core.windows.net/some/file/x");
    StorageLocation wasbLocation =
        StorageLocation.of("wasb://container@acc.blob.core.windows.net/some/file");
    soft.assertThat(abfsLocation).isNotEqualTo(wasbLocation);
    soft.assertThat(abfsLocation.isChildOf(wasbLocation)).isTrue();
  }

  @Test
  public void testLocationComparisons() {
    StorageLocation location =
        StorageLocation.of("abfss://container-dash@acc.blob.core.windows.net/some_file/metadata");
    StorageLocation parentLocation =
        StorageLocation.of("abfss://container-dash@acc.blob.core.windows.net");
    StorageLocation parentLocationTrailingSlash =
        StorageLocation.of("abfss://container-dash@acc.blob.core.windows.net/");

    soft.assertThat(location).isNotEqualTo(parentLocation);
    soft.assertThat(location).isNotEqualTo(parentLocationTrailingSlash);

    soft.assertThat(location.isChildOf(parentLocation)).isTrue();
    soft.assertThat(location.isChildOf(parentLocationTrailingSlash)).isTrue();
  }

  @Test
  public void testLocation_negative_cases() {
    soft.assertThatThrownBy(
            () -> StorageLocation.of("abfss://storageaccount.blob.core.windows.net/myfile"))
        .isInstanceOf(IllegalArgumentException.class);
    soft.assertThatThrownBy(() -> StorageLocation.of("abfss://container@storageaccount/myfile"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
