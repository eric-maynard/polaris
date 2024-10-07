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

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.FileIO;

/**
 * A FileIOFactory implementation that delegates to a list of other FileIOFactory implementations
 */
@JsonTypeName("delegating")
public class DelegatingFileIOFactory extends FileIOFactory {
  private final List<FileIOFactory> factories;

  public DelegatingFileIOFactory(List<FileIOFactory> factories) {
    if (factories == null || factories.isEmpty()) {
      throw new IllegalArgumentException(
          "DelegatingFileIOFactory requires at least one FileIOFactory");
    }
    this.factories = factories;
  }

  @Override
  public FileIO loadFileIO(String impl, Map<String, String> properties) {
    FileIOFactory tailFactory = factories.getLast();
    List<SupportsFileIOWrapping> wrappers =
        factories.subList(1, factories.size() - 1).reversed().stream()
            .map(
                factory -> {
                  if (factory instanceof SupportsFileIOWrapping wrapper) {
                    return wrapper;
                  } else {
                    throw new IllegalArgumentException(
                        String.format(
                            "FileIOFactory %s does not support delegation",
                            factory.getClass().getSimpleName()));
                  }
                })
            .toList();

    FileIO fileIO = tailFactory.loadFileIO(impl, properties);
    for (SupportsFileIOWrapping wrapper : wrappers) {
      fileIO = wrapper.wrap(fileIO, properties);
    }
    return fileIO;
  }
}
