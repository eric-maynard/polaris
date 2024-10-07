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
package org.apache.polaris.service.io;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record IOConfiguration(
    @JsonProperty("factoryType") String factoryType,
    @JsonProperty("factoryTypes") List<FileIOFactoryConfiguration> factoryTypes) {

  /** Build the correct {@link FileIOFactory} given this configuration} */
  public FileIOFactory buildFileIOFactory() {
    if (factoryType == null && !hasFactoryTypes()) {
      throw new IllegalArgumentException("Either factoryType or factoryTypes is required!");
    } else if (factoryType != null && hasFactoryTypes()) {
      throw new IllegalArgumentException(
          "Both factoryType and factoryTypes were provided; choose one!");
    } else if (!hasFactoryTypes()) {
      return FileIOFactory.loadFileIOFactory(factoryType);
    } else {
      return new DelegatingFileIOFactory(
          factoryTypes.stream().map(FileIOFactoryConfiguration::getFileIOFactory).toList());
    }
  }

  private boolean hasFactoryTypes() {
    return factoryTypes != null && !factoryTypes.isEmpty();
  }
}
