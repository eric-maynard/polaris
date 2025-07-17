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
package org.apache.polaris.extension.conversion.xtable;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.service.conversion.TableConverter;
import org.apache.polaris.service.conversion.TableFormat;
import org.apache.polaris.service.types.GenericTable;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.sync.SyncMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TableConverter} implementation that uses XTable to convert a table locally. Since
 * conversion happens within the JVM, this should only be used for testing. Simply uses a default
 * Hadoop Configuration for File IO.
 */
@ApplicationScoped
@Identifier("xtable")
public class XTableConverter implements TableConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(XTableConverter.class);

  private final ConversionController conversionController;
  private final Configuration hadoopConfiguration;

  public XTableConverter() {
    this(null);
  }

  public XTableConverter(Configuration configuration) {
    if (configuration == null) {
      hadoopConfiguration = newLocalSparkConfiguration();
    } else {
      hadoopConfiguration = configuration;
    }
    conversionController = new ConversionController(hadoopConfiguration);
  }

  private Configuration newLocalSparkConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set("spark.master", "local[1]");
    return configuration;
  }

  @Override
  public Optional<GenericTable> convert(
      GenericTable table,
      TableFormat targetFormat,
      Map<String, String> storageCredentials,
      int requestedFreshnessSeconds) {
    // TODO remove debug printlns
    System.out.println("#### Attempting to convert: " + table);
    String targetLocation = table.getBaseLocation() + "/_" + targetFormat.toString();
    try {
      SourceTable sourceTable =
          new SourceTable(
              table.getName(),
              table.getFormat(),
              table.getBaseLocation(),
              /* namespace= */ null,
              /* catalogConfig= */ null,
              /* metadataRetention= */ null,
              new Properties());

      TargetTable targetTable =
          new TargetTable(
              table.getName(),
              targetFormat.toString(),
              targetLocation,
              /* namespace= */ null,
              /* catalogConfig= */ null,
              /* metadataRetention= */ null,
              new Properties());

      ConversionConfig conversionConfig =
          ConversionConfig.builder()
              .sourceTable(sourceTable)
              .targetTables(List.of(targetTable))
              .syncMode(SyncMode.FULL)
              .build();

      final ConversionSourceProvider<?> provider;
      if (table.getFormat().equals(TableFormat.ICEBERG.getValue())) {
        provider = new IcebergConversionSourceProvider();
      } else if (table.getFormat().equals(TableFormat.DELTA.getValue())) {
        provider = new DeltaConversionSourceProvider();
      } else if (table.getFormat().equals(TableFormat.HUDI.getValue())) {
        provider = new HudiConversionSourceProvider();
      } else {
        LOGGER.debug("Xtable cannot convert from source format: {}", table.getFormat());
        return Optional.empty();
      }

      provider.init(hadoopConfiguration);
      var commits = conversionController.sync(conversionConfig, provider);
      System.out.println("#### Commits: ");
      for (var entry : commits.entrySet()) {
        System.out.println("  #### " + entry.getKey() + " -> " + entry.getValue());
      }

      return Optional.of(
          new GenericTable(
              table.getName(),
              targetFormat.toString(),
              targetLocation,
              table.getDoc() + String.format(" (Converted via XTable to %s)", targetFormat),
              table.getProperties()));
    } catch (RuntimeException e) {
      LOGGER.info("Encountered an error during table conversion: " + e.getMessage());
      return Optional.empty();
    }
  }

  /** Load Iceberg metadata directly using the HadoopFileIO */
  @Override
  public Optional<TableMetadata> loadIcebergTable(String icebergLocation) {
    try (FileIO fileIO = new HadoopFileIO(hadoopConfiguration)) {
      return Optional.of(TableMetadataParser.read(fileIO, icebergLocation));
    }
  }
}
