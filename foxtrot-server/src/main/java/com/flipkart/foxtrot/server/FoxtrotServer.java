/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.KubernetesClusterDiscoveryConfig;
import com.flipkart.foxtrot.core.querystore.impl.MarathonClusterDiscoveryConfig;
import com.flipkart.foxtrot.core.querystore.impl.SimpleClusterDiscoveryConfig;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.di.FoxtrotModule;
import com.google.inject.Stage;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.oor.OorBundle;
import io.dropwizard.server.AbstractServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import ru.vyarus.dropwizard.guice.GuiceBundle;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.util.EnumSet;


/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:38 PM
 */
public class FoxtrotServer extends Application<FoxtrotServerConfiguration> {

    @Override
    public void initialize(Bootstrap<FoxtrotServerConfiguration> bootstrap) {
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(), new EnvironmentVariableSubstitutor(false)));
        bootstrap.addBundle(new AssetsBundle("/console/echo/", "/", "browse-events.htm", "console"));
        bootstrap.addBundle(new OorBundle<FoxtrotServerConfiguration>() {
            public boolean withOor() {
                return false;
            }
        });

        final SwaggerBundleConfiguration swaggerBundleConfiguration = getSwaggerBundleConfiguration();

        bootstrap.addBundle(new SwaggerBundle<FoxtrotServerConfiguration>() {
            @Override
            protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(FoxtrotServerConfiguration configuration) {
                return swaggerBundleConfiguration;
            }
        });

        bootstrap.addBundle(GuiceBundle.<FoxtrotServerConfiguration>builder()
                                    .enableAutoConfig("com.flipkart.foxtrot")
                                    .modules(
                                            new FoxtrotModule())
                                    .useWebInstallers()
                                    .printDiagnosticInfo()
                                    .build(Stage.PRODUCTION));
        bootstrap.addCommand(new InitializerCommand());
        configureObjectMapper(bootstrap.getObjectMapper());
    }

    @Override
    public String getName() {
        return "foxtrot";
    }

    @Override
    public void run(FoxtrotServerConfiguration configuration, Environment environment) throws Exception {
        // Enable CORS headers
        final FilterRegistration.Dynamic cors = environment.servlets()
                .addFilter("CORS", CrossOriginFilter.class);

        // Configure CORS parameters
        cors.setInitParameter("allowedOrigins", "*");
        cors.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin");
        cors.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD");

        // Add URL mapping
        cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");

        ((AbstractServerFactory)configuration.getServerFactory()).setJerseyRootPath("/foxtrot");

        MetricUtil.setup(environment.metrics());
        ElasticsearchUtils.setTableNamePrefix(configuration.getElasticsearch());

    }

    private void configureObjectMapper(ObjectMapper objectMapper) {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        objectMapper.registerSubtypes(new NamedType(SimpleClusterDiscoveryConfig.class, "foxtrot_simple"));
        objectMapper.registerSubtypes(new NamedType(MarathonClusterDiscoveryConfig.class, "foxtrot_marathon"));
        objectMapper.registerSubtypes(new NamedType(KubernetesClusterDiscoveryConfig.class, "foxtrot_kubernetes"));
    }

    private SwaggerBundleConfiguration getSwaggerBundleConfiguration() {
        final SwaggerBundleConfiguration swaggerBundleConfiguration = new SwaggerBundleConfiguration();
        swaggerBundleConfiguration.setTitle("Foxtrot");
        swaggerBundleConfiguration.setResourcePackage("com.flipkart.foxtrot.server.resources");
        swaggerBundleConfiguration.setUriPrefix("/foxtrot");
        swaggerBundleConfiguration.setDescription("A store abstraction and analytics system for real-time event data.");
        return swaggerBundleConfiguration;
    }

}
