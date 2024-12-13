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
package org.apache.polaris.core.tables;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.entity.ForeignTableEntity;
import org.apache.polaris.core.entity.TableLikeEntity;
import org.apache.xtable.model.storage.TableFormat;

public interface ForeignTableConverter {

  Table convert(ForeignTableEntity entity) throws ConversionFailureException;

  static LoadTableResponse loadTable(ForeignTableEntity entity) {
    ForeignTableEntity sourceStrippedEntity = new ForeignTableEntity.Builder(entity).setSource(null).build();
    if (TableFormat.DELTA.equalsIgnoreCase(entity.getSource())) {
      Table table = new DeltaTableConverter().convert(sourceStrippedEntity);
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .build();
    }

    throw new ConversionFailureException("Invalid source format: " + entity.getSource());
  }
}
