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
package io.polaris.core;

public class PolarisConfiguration<T> {

  public final String key;
  public final String description;
  public final T defaultValue;
  public final boolean catalogConfig;

  public PolarisConfiguration(String key, String description, T defaultValue, boolean catalogConfig) {
    this.key = key;
    this.description = description;
    this.defaultValue = defaultValue;
    this.catalogConfig = catalogConfig;
  }

  public static class Builder<T> {
    private String key;
    private String description;
    private T defaultValue;
    private boolean catalogConfig = false;

    public Builder<T> key(String key) {
      this.key = key;
      return this;
    }

    public Builder<T> description(String description) {
      this.description = description;
      return this;
    }

    public Builder<T> defaultValue(T defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    public Builder<T> catalogConfig(boolean catalogConfig) {
      this.catalogConfig = catalogConfig;
      return this;
    }

    public PolarisConfiguration<T> build() {
      if (key == null || description == null || defaultValue == null) {
        throw new IllegalArgumentException("key, description, and defaultValue are required");
      }
      return new PolarisConfiguration<>(key, description, defaultValue, catalogConfig);
    }
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static final PolarisConfiguration<Boolean>
      ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING =
          PolarisConfiguration.<Boolean>builder()
              .key("ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING")
              .description(
                  "If set to true, require that principals must rotate their credentials before being used " +
                  "for anything else."
              )
              .defaultValue(false)
              .build();

  public static final PolarisConfiguration<Boolean> ALLOW_TABLE_LOCATION_OVERLAP =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_TABLE_LOCATION_OVERLAP")
          .description(
              "If set to true, allow one table's location to reside within another table's location. " +
              "This is only enforced within a given namespace."
          )
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> ALLOW_NAMESPACE_LOCATION_OVERLAP =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_NAMESPACE_LOCATION_OVERLAP")
          .description(
              "If set to true, allow one table's location to reside within another table's location. " +
              "This is only enforced within a parent catalog or namespace."
          )
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> ALLOW_EXTERNAL_METADATA_FILE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_EXTERNAL_METADATA_FILE_LOCATION")
          .description(
              "If set to true, allows metadata files to be located outside the default metadata directory."
          )
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> ENFORCE_GLOBALLY_UNIQUE_TABLE_LOCATIONS =
      PolarisConfiguration.<Boolean>builder()
          .key("ENFORCE_GLOBALLY_UNIQUE_TABLE_LOCATIONS")
          .description(
              "If set to true, enforces that all table locations must be globally unique. " +
              "This is enforced across all namespaces and catalogs."
          )
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean>
      ENFORCE_TABLE_LOCATIONS_INSIDE_NAMESPACE_LOCATIONS =
          PolarisConfiguration.<Boolean>builder()
              .key("ENFORCE_TABLE_LOCATIONS_INSIDE_NAMESPACE_LOCATIONS")
              .description(
                  "If set to true, enforces that table locations must reside within their immediate parent " +
                  "namespace's locations."
              )
              .defaultValue(true)
              .build();

  public static final PolarisConfiguration<Boolean> ALLOW_OVERLAPPING_CATALOG_URLS =
      PolarisConfiguration.<Boolean>builder()
          .key("ALLOW_OVERLAPPING_CATALOG_URLS")
          .description(
              """
              If set to true, allows catalog URLs to overlap.
              """.trim())
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> CATALOG_ALLOW_UNSTRUCTURED_TABLE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .catalogConfig(true)
          .key("allow.unstructured.table.location")
          .description(
              """
              If set to true, allows unstructured table locations.
              """.trim())
          .defaultValue(false)
          .build();

  public static final PolarisConfiguration<Boolean> CATALOG_ALLOW_EXTERNAL_TABLE_LOCATION =
      PolarisConfiguration.<Boolean>builder()
          .catalogConfig(true)
          .key("allow.external.table.location")
          .description(
              """
              If set to true, allows tables to have external locations outside the default structure.
              """
                  .trim())
          .defaultValue(false)
          .build();
}
