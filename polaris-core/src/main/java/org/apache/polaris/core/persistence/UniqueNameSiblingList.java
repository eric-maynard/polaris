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
package org.apache.polaris.core.persistence;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.PolarisEntityType;

public class UniqueNameSiblingList {

  /**
   * Given the type code of an {@link PolarisEntityType}, return the sibling types which the
   * configuration {@link FeatureConfiguration#NAME_CONFLICT_SIBLING_TYPES} says are "name conflict
   * siblings" of the provided type. These entity types cannot share a name when they share a
   * parent.
   */
  public static List<Integer> get(PolarisCallContext polarisCallContext, int entityTypeCode) {
    return get(polarisCallContext, PolarisEntityType.fromCode(entityTypeCode)).stream()
        .map(t -> t.getCode())
        .collect(Collectors.toList());
  }

  /**
   * Given a {@link PolarisEntityType}, return the sibling types which the configuration {@link
   * FeatureConfiguration#NAME_CONFLICT_SIBLING_TYPES} says are "name conflict siblings" of the
   * provided type. These entity types cannot share a name when they share a parent.
   */
  public static List<PolarisEntityType> get(
      PolarisCallContext polarisCallContext, PolarisEntityType entityType) {

    List<List<Integer>> configuredMappings =
        polarisCallContext
            .getConfigurationStore()
            .getConfiguration(polarisCallContext, FeatureConfiguration.NAME_CONFLICT_SIBLING_TYPES);

    Set<Integer> mappedTypeCodes = new HashSet<>();
    mappedTypeCodes.add(entityType.getCode());

    for (List<Integer> mapping : configuredMappings) {
      if (mapping.contains(entityType.getCode())) {
        mappedTypeCodes.addAll(mapping);
      }
    }

    return mappedTypeCodes.stream().map(PolarisEntityType::fromCode).collect(Collectors.toList());
  }
}
