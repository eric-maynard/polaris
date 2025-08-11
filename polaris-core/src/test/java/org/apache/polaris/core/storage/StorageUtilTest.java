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
package org.apache.polaris.core.storage;

import java.util.Arrays;
import java.util.Map;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public class StorageUtilTest {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void testEmptyString() {
    soft.assertThat(StorageUtil.getBucket("")).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3", "s3a", "gcs", "abfs", "wasb", "file"})
  public void testAbsolutePaths(String scheme) {
    soft.assertThat(StorageUtil.getBucket(scheme + "://bucket/path/file.txt")).isEqualTo("bucket");
    soft.assertThat(StorageUtil.getBucket(scheme + "://bucket:with:colon/path/file.txt"))
        .isEqualTo("bucket:with:colon");
    soft.assertThat(StorageUtil.getBucket(scheme + "://bucket_with_underscore/path/file.txt"))
        .isEqualTo("bucket_with_underscore");
    soft.assertThat(StorageUtil.getBucket(scheme + "://bucket_with_ユニコード/path/file.txt"))
        .isEqualTo("bucket_with_ユニコード");
  }

  @Test
  public void testRelativePaths() {
    soft.assertThat(StorageUtil.getBucket("bucket/path/file.txt")).isNull();
    soft.assertThat(StorageUtil.getBucket("path/file.txt")).isNull();
  }

  @Test
  public void testAbsolutePathWithoutScheme() {
    soft.assertThat(StorageUtil.getBucket("/bucket/path/file.txt")).isNull();
  }

  @Test
  public void testInvalidURI() {
    soft.assertThatThrownBy(() -> StorageUtil.getBucket("s3://bucket with space/path/file.txt"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testAuthorityWithPort() {
    soft.assertThat(StorageUtil.getBucket("s3://bucket:8080/path/file.txt"))
        .isEqualTo("bucket:8080");
  }

  @Test
  public void getLocationsAllowedToBeAccessed() {
    soft.assertThat(StorageUtil.getLocationsAllowedToBeAccessed(null, Map.of())).isEmpty();
    soft.assertThat(StorageUtil.getLocationsAllowedToBeAccessed("", Map.of())).isNotEmpty();
    soft.assertThat(StorageUtil.getLocationsAllowedToBeAccessed("/foo/", Map.of()))
        .containsExactlyInAnyOrder("/foo/");
    soft.assertThat(
            StorageUtil.getLocationsAllowedToBeAccessed(
                "/foo/",
                Map.of(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, "/foo/")))
        .containsExactlyInAnyOrder("/foo/");
    soft.assertThat(
            StorageUtil.getLocationsAllowedToBeAccessed(
                "/foo/",
                Map.of(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, "/bar/")))
        .containsExactlyInAnyOrder("/foo/", "/bar/");
    soft.assertThat(
            StorageUtil.getLocationsAllowedToBeAccessed(
                "/foo/",
                Map.of(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, "/foo/bar/")))
        .containsExactlyInAnyOrder("/foo/");
    soft.assertThat(
            StorageUtil.getLocationsAllowedToBeAccessed(
                "/foo/bar/",
                Map.of(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, "/foo/")))
        .containsExactlyInAnyOrder("/foo/");
    soft.assertThat(
            StorageUtil.getLocationsAllowedToBeAccessed(
                "/foo/bar/",
                Map.of(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY, "/foo/")))
        .containsExactlyInAnyOrder("/foo/");
    soft.assertThat(
            StorageUtil.getLocationsAllowedToBeAccessed(
                "/1/",
                Map.of(
                    IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, "/2/",
                    IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY, "/3/")))
        .containsExactlyInAnyOrder("/1/", "/2/", "/3/");
  }

  @Test
  public void removeRedundantLocations() {
    soft.assertThat(
            StorageUtil.removeRedundantLocations(
                Arrays.asList(
                    "", // yielded
                    //
                    "s3://foo/bar/baz/",
                    "s3://foo/bar/baz",
                    "gs://foo/bar/baz/", // yielded
                    "s3://foo/bar/", // yielded
                    "s3://foo/bar/",
                    "gs://bar/baz/foo/blah",
                    "gs://bar/baz/fooblah", // yielded
                    "gs://bar/baz/foo", // yielded
                    //
                    null,
                    //
                    "abfs://foo:bar@host.domain/path/here", // yielded
                    "abfss://foo:bar@host.domain/path/here",
                    "wasb://foo:bar@host.domain/path/here",
                    "wasbs://foo:bar@host.domain/path/here",
                    //
                    "abfs://other:foo@host.domain/path/here", // yielded
                    //
                    "wasbs://other:foo@somwhere.else/path/here" // yielded
                    )))
        .containsExactlyInAnyOrder(
            "",
            "gs://foo/bar/baz/",
            "s3://foo/bar/",
            "abfs://foo:bar@host.domain/path/here",
            "abfs://other:foo@host.domain/path/here",
            "gs://bar/baz/fooblah",
            "gs://bar/baz/foo",
            "wasbs://other:foo@somwhere.else/path/here");
  }
}
