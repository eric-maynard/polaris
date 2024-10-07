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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.io.FileIO;

/**
 * Interface for providing a way to construct FileIO objects, such as for reading/writing S3.
 */
public abstract class FileIOFactory implements Discoverable {
  private Map<String, String> properties = new HashMap<>();

  public abstract FileIO loadFileIO(String impl, Map<String, String> properties);

  public final void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public final Map<String, String> getProperties() {
    return properties;
  }

  protected static Map<String, String> mergeMaps(
      Map<String, String> oldMap, Map<String, String> newMap) {
    if (oldMap == null) {
      return newMap;
    } else if (newMap == null) {
      return oldMap;
    }

    Map<String, String> mergedMap = new HashMap<>(oldMap);
    mergedMap.putAll(newMap);
    return mergedMap;
  }
}
