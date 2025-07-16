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

plugins {
  id("polaris-server")
  alias(libs.plugins.jandex)
}

val sparkMajorVersion = "3.5"
val scalaVersion = "2.12"
val icebergVersion = pluginlibs.versions.iceberg.get()
val spark35Version = pluginlibs.versions.spark35.get()
val scalaLibraryVersion = pluginlibs.versions.scala212.get()

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-service-common"))
  implementation(project(":polaris-api-catalog-service"))

  implementation(libs.slf4j.api)

  // XTable core
  implementation("org.apache.xtable:xtable-core_2.12:0.3.0-incubating")

  // Required for Delta source support
  compileOnly("org.scala-lang:scala-library:${scalaLibraryVersion}")
  implementation("org.apache.iceberg:iceberg-spark-runtime-3.5_${scalaVersion}:${icebergVersion}")
  implementation("org.apache.spark:spark-sql_${scalaVersion}:${spark35Version}") {
    // exclude log4j dependencies. Explicit dependencies for the log4j libraries are
    // enforced below to ensure the version compatibility
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.apache.logging.log4j", "log4j-core")
    exclude("org.slf4j", "jul-to-slf4j")
  }
  testImplementation("io.delta:delta-spark_${scalaVersion}:3.3.1")

  // Hadoop
  implementation("org.apache.hadoop:hadoop-common:3.3.6")

  // Quarkus + Compile-only dependencies
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.smallrye.common.annotation) // @Identifier
  compileOnly(libs.smallrye.config.core) // @ConfigMapping

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-core")
}

description = "Implements table conversion via XTable"
