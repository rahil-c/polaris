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

plugins { id("polaris-server") }

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-service-common"))
  implementation(project(":polaris-api-catalog-service"))

  implementation(libs.slf4j.api)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.smallrye.common.annotation) // @Identifier
  compileOnly(libs.smallrye.config.core) // @ConfigMapping

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-core")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.google.guava:guava:32.1.3-jre")

  // ─────────── Add Lombok here ───────────
  compileOnly("org.projectlombok:lombok:1.18.30") // make Lombok available at compile time
  annotationProcessor("org.projectlombok:lombok:1.18.30") // trigger Lombok’s annotation-processing
  testCompileOnly("org.projectlombok:lombok:1.18.30")
  testAnnotationProcessor("org.projectlombok:lombok:1.18.30")
}

description = "Implements table conversion via XTable"
