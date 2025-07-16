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
package org.apache.polaris.service.conversion;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.TableMetadata;
import org.apache.polaris.service.types.GenericTable;

@ApplicationScoped
@Identifier("none")
public class NoneTableConverter implements TableConverter {

  @Override
  public Optional<GenericTable> convert(
      GenericTable table,
      TableFormat targetFormat,
      Map<String, String> storageCredentials,
      int requestedFreshnessSeconds) {
    return Optional.empty();
  }

  @Override
  public Optional<TableMetadata> loadIcebergTable(String icebergLocation) {
    throw new IllegalStateException("NONE converter shouldn't be used to load Iceberg data");
  }
}
