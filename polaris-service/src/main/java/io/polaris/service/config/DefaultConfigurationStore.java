/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.service.config;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.PolarisConfiguration;
import io.polaris.core.PolarisConfigurationStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

public class DefaultConfigurationStore implements PolarisConfigurationStore {
  private final Map<String, Object> properties;

  public DefaultConfigurationStore(Map<String, Object> properties) {
    this.properties = properties;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> @Nullable T getConfiguration(PolarisCallContext ctx, String configName) {
    return (T) properties.get(configName);
  }
}
