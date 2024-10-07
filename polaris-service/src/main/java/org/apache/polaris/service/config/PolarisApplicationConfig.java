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
package org.apache.polaris.service.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.dropwizard.core.Configuration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.auth.DiscoverableAuthenticator;
import org.apache.polaris.service.context.CallContextResolver;
import org.apache.polaris.service.context.RealmContextResolver;
import org.apache.polaris.service.io.IOConfiguration;
import org.apache.polaris.service.ratelimiter.RateLimiter;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Configuration specific to a Polaris REST Service. Place these entries in a YML file for them to
 * be picked up, i.e. `iceberg-rest-server.yml`
 */
public class PolarisApplicationConfig extends Configuration {
  private MetaStoreManagerFactory metaStoreManagerFactory;
  private String defaultRealm = "default-realm";
  private RealmContextResolver realmContextResolver;
  private CallContextResolver callContextResolver;
  private DiscoverableAuthenticator<String, AuthenticatedPolarisPrincipal> polarisAuthenticator;
  private CorsConfiguration corsConfiguration = new CorsConfiguration();
  private TaskHandlerConfiguration taskHandler = new TaskHandlerConfiguration();
  private PolarisConfigurationStore configurationStore =
      new DefaultConfigurationStore(new HashMap<>());
  private List<String> defaultRealms;
  private String awsAccessKey;
  private String awsSecretKey;
  private org.apache.polaris.service.io.IOConfiguration IOConfiguration;
  private RateLimiter rateLimiter;

  public static final long REQUEST_BODY_BYTES_NO_LIMIT = -1;
  private long maxRequestBodyBytes = REQUEST_BODY_BYTES_NO_LIMIT;

  @JsonProperty("metaStoreManager")
  public void setMetaStoreManagerFactory(MetaStoreManagerFactory metaStoreManagerFactory) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  @JsonProperty("metaStoreManager")
  public MetaStoreManagerFactory getMetaStoreManagerFactory() {
    return metaStoreManagerFactory;
  }

  @JsonProperty("io")
  public void setIOConfiguration(IOConfiguration IOConfiguration) {
    this.IOConfiguration = IOConfiguration;
  }

  @JsonProperty("io")
  public IOConfiguration getIOConfiguration() {
    return IOConfiguration;
  }

  @JsonProperty("authenticator")
  public void setPolarisAuthenticator(
      DiscoverableAuthenticator<String, AuthenticatedPolarisPrincipal> polarisAuthenticator) {
    this.polarisAuthenticator = polarisAuthenticator;
  }

  public DiscoverableAuthenticator<String, AuthenticatedPolarisPrincipal>
      getPolarisAuthenticator() {
    return polarisAuthenticator;
  }

  public RealmContextResolver getRealmContextResolver() {
    return realmContextResolver;
  }

  public void setRealmContextResolver(RealmContextResolver realmContextResolver) {
    this.realmContextResolver = realmContextResolver;
  }

  public CallContextResolver getCallContextResolver() {
    return callContextResolver;
  }

  @JsonProperty("callContextResolver")
  public void setCallContextResolver(CallContextResolver callContextResolver) {
    this.callContextResolver = callContextResolver;
  }

  private OAuth2ApiService oauth2Service;

  @JsonProperty("oauth2")
  public void setOauth2Service(OAuth2ApiService oauth2Service) {
    this.oauth2Service = oauth2Service;
  }

  public OAuth2ApiService getOauth2Service() {
    return oauth2Service;
  }

  public String getDefaultRealm() {
    return defaultRealm;
  }

  @JsonProperty("defaultRealm")
  public void setDefaultRealm(String defaultRealm) {
    this.defaultRealm = defaultRealm;
  }

  @JsonProperty("cors")
  public CorsConfiguration getCorsConfiguration() {
    return corsConfiguration;
  }

  @JsonProperty("cors")
  public void setCorsConfiguration(CorsConfiguration corsConfiguration) {
    this.corsConfiguration = corsConfiguration;
  }

  @JsonProperty("rateLimiter")
  public RateLimiter getRateLimiter() {
    return rateLimiter;
  }

  @JsonProperty("rateLimiter")
  public void setRateLimiter(@Nullable RateLimiter rateLimiter) {
    this.rateLimiter = rateLimiter;
  }

  public void setTaskHandler(TaskHandlerConfiguration taskHandler) {
    this.taskHandler = taskHandler;
  }

  public TaskHandlerConfiguration getTaskHandler() {
    return taskHandler;
  }

  @JsonProperty("featureConfiguration")
  public void setFeatureConfiguration(Map<String, Object> featureConfiguration) {
    this.configurationStore = new DefaultConfigurationStore(featureConfiguration);
  }

  @JsonProperty("maxRequestBodyBytes")
  public void setMaxRequestBodyBytes(long maxRequestBodyBytes) {
    // The underlying library that we use to implement the limit treats all values <= 0 as the
    // same, so we block all but -1 to prevent ambiguity.
    Preconditions.checkArgument(
        maxRequestBodyBytes == -1 || maxRequestBodyBytes > 0,
        "maxRequestBodyBytes must be a positive integer or %s to specify no limit.",
        REQUEST_BODY_BYTES_NO_LIMIT);

    this.maxRequestBodyBytes = maxRequestBodyBytes;
  }

  public long getMaxRequestBodyBytes() {
    return maxRequestBodyBytes;
  }

  public PolarisConfigurationStore getConfigurationStore() {
    return configurationStore;
  }

  public List<String> getDefaultRealms() {
    return defaultRealms;
  }

  public AwsCredentialsProvider credentialsProvider() {
    if (StringUtils.isNotBlank(awsAccessKey) && StringUtils.isNotBlank(awsSecretKey)) {
      LoggerFactory.getLogger(PolarisApplicationConfig.class)
          .warn("Using hard-coded AWS credentials - this is not recommended for production");
      return StaticCredentialsProvider.create(
          AwsBasicCredentials.create(awsAccessKey, awsSecretKey));
    }
    return null;
  }

  public void setAwsAccessKey(String awsAccessKey) {
    this.awsAccessKey = awsAccessKey;
  }

  public void setAwsSecretKey(String awsSecretKey) {
    this.awsSecretKey = awsSecretKey;
  }

  public void setDefaultRealms(List<String> defaultRealms) {
    this.defaultRealms = defaultRealms;
  }
}
