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
package org.apache.polaris.service.quarkus.config;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.polaris.service.conversion.NoneTableConverter;
import org.apache.polaris.service.conversion.TableConverter;
import org.apache.polaris.service.conversion.TableConverterRegistry;
import org.apache.polaris.service.conversion.TableFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class QuarkusTableConverterRegistry implements TableConverterRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuarkusTableConverterRegistry.class);

  private final Map<Pair<TableFormat, TableFormat>, TableConverter> converterMap = new HashMap<>();
  private static final Pattern configPattern = Pattern.compile("(.+?).to.(.+?)");

  @Inject
  @Identifier("none")
  NoneTableConverter noneTableConverter;

  @Inject
  public QuarkusTableConverterRegistry(
      QuarkusConverterConfig config, @Any Instance<TableConverter> converters) {
    Map<String, TableConverter> beansById =
        converters.stream()
            .collect(
                Collectors.toMap(
                    converter ->
                        converter
                            .getClass()
                            .getSuperclass()
                            .getAnnotation(Identifier.class)
                            .value(),
                    Function.identity()));

    config
        .converters()
        .forEach(
            (key, identifier) -> {
              TableConverter converter = beansById.get(identifier);
              if (converter != null) {
                converterMap.put(getTableFormats(key), converter);
              } else {
                LOGGER.error("Unable to load converter: {}", identifier);
              }
            });
  }

  private Pair<TableFormat, TableFormat> getTableFormats(String configEntry) {
    Matcher matcher = configPattern.matcher(configEntry);
    if (matcher.matches()) {
      TableFormat sourceFormat = TableFormat.of(matcher.group(1));
      TableFormat targetFormat = TableFormat.of(matcher.group(2));
      return Pair.of(sourceFormat, targetFormat);
    } else {
      throw new IllegalArgumentException("Could not parse converter configuration: " + configEntry);
    }
  }

  /** Load the TableConverter for a source/target format pair */
  @Override
  public TableConverter getConverter(TableFormat sourceFormat, TableFormat targetFormat) {
    return converterMap.getOrDefault(Pair.of(sourceFormat, targetFormat), noneTableConverter);
  }

  /** Return all registered converters */
  public Set<TableConverter> getConverters() {
    return new HashSet<>(converterMap.values());
  }
}
