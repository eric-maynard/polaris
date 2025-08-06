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

import jakarta.annotation.Nonnull;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;

public class StorageUtil {
  /**
   * Concatenating two file paths by making sure one and only one path separator is placed between
   * the two paths.
   *
   * @param leftPath left path
   * @param rightPath right path
   * @param fileSep File separator to use.
   * @return Well formatted file path.
   */
  public static @Nonnull String concatFilePrefixes(
      @Nonnull String leftPath, String rightPath, String fileSep) {
    if (leftPath.endsWith(fileSep) && rightPath.startsWith(fileSep)) {
      return leftPath + rightPath.substring(1);
    } else if (!leftPath.endsWith(fileSep) && !rightPath.startsWith(fileSep)) {
      return leftPath + fileSep + rightPath;
    } else {
      return leftPath + rightPath;
    }
  }

  /**
   * Given a path, extract the bucket (authority).
   *
   * @param path A path to parse
   * @return The bucket/authority of the path
   */
  public static @Nonnull String getBucket(String path) {
    URI uri = URI.create(path);
    return getBucket(uri);
  }

  /**
   * Given a URI, extract the bucket (authority).
   *
   * @param uri A path to parse
   * @return The bucket/authority of the URI
   */
  public static @Nonnull String getBucket(URI uri) {
    return uri.getAuthority();
  }

  /** Given a TableMetadata, extracts the locations where the table's [meta]data might be found. */
  public static @Nonnull Set<String> getLocationsAllowedToBeAccessed(TableMetadata tableMetadata) {
    return getLocationsAllowedToBeAccessed(tableMetadata.location(), tableMetadata.properties());
  }

  /** Given a baseLocation and entity (table?) properties, extracts the relevant locations */
  public static @Nonnull Set<String> getLocationsAllowedToBeAccessed(
      String baseLocation, Map<String, String> properties) {
    List<String> locations = new ArrayList<>();
    locations.add(baseLocation);
    locations.add(properties.get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY));
    locations.add(
        properties.get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY));
    return removeRedundantLocations(locations);
  }

  /** Given a ViewMetadata, extracts the locations where the view's [meta]data might be found. */
  public static @Nonnull Set<String> getLocationsAllowedToBeAccessed(ViewMetadata viewMetadata) {
    return Set.of(viewMetadata.location());
  }

  /**
   * Removes "redundant" locations, so {@code "/a/b/", "/a/b/c", "/a/b/d"} will be reduced to just
   * {@code "/a/b/"}, using {@link StorageLocation#isChildOf(StorageLocation)} to identify children.
   *
   * <p>Whether a location's scheme is considered, depends on the object-storage type specific
   * implementation.
   *
   * <p>S3 storage locations will be reduced irrespective of whether the scheme is {@code s3} or
   * {@code s3a}. For example, {@code s3a://bucket/path/inner} will not be returned, if the input
   * contains {@code s3://bucket/path/}.
   *
   * <p>Azure storage locations will be reduced irrespective of whether the scheme is {@code abfs},
   * {@code abfss}, {@code wasb} or {@code wasbs}. For example, {@code wasbs://u:p@h.d/path/inner}
   * will not be returned, if the input contains {@code abfs://u:p@h.d/path/}.
   */
  static @Nonnull Set<String> removeRedundantLocations(List<String> locationStrings) {
    int numLocations = locationStrings.size();

    StorageLocation[] storageLocations = new StorageLocation[numLocations];
    boolean[] skip = new boolean[numLocations];

    for (int i = 0; i < numLocations; i++) {
      String location = locationStrings.get(i);
      if (location == null) {
        // Saves some null-checks below.
        skip[i] = true;
      } else {
        storageLocations[i] = StorageLocation.of(location);
      }
    }

    for (int outer = 0; outer < numLocations; outer++) {
      // skip known children + 'null' locations
      if (skip[outer]) {
        continue;
      }
      var outerLocation = storageLocations[outer];
      for (int inner = 0; inner < numLocations; inner++) {
        // skip "outer" and 'null' location, known children
        if (inner == outer || skip[inner]) {
          continue;
        }
        var innerLocation = storageLocations[inner];
        if (innerLocation.isChildOf(outerLocation)) {
          // "inner" location is a child, exclude in result
          skip[inner] = true;
        }
      }
    }

    // Using streams here, the code's more concise.
    return IntStream.range(0, numLocations)
        .filter(i -> !skip[i])
        .mapToObj(locationStrings::get)
        .collect(Collectors.toSet());
  }
}
