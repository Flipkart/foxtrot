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

import com.flipkart.foxtrot.core.common.DataDeletionManager;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.server.cluster.ClusterManager;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.guice.FoxtrotModule;
import com.flipkart.foxtrot.server.providers.FlatResponseCsvProvider;
import com.flipkart.foxtrot.server.providers.FlatResponseErrorTextProvider;
import com.flipkart.foxtrot.server.providers.FlatResponseTextProvider;
import com.flipkart.foxtrot.server.util.ManagedActionScanner;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.assets.AssetsBundle;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.metrics.core.HealthCheck;
import net.sourceforge.cobertura.CoverageIgnore;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.reflections.Reflections;

import javax.ws.rs.Path;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Set;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:38 PM
 */

@CoverageIgnore
public class FoxtrotServer extends Service<FoxtrotServerConfiguration> {
    @Override
    public void initialize(Bootstrap<FoxtrotServerConfiguration> bootstrap) {
        bootstrap.setName("foxtrot");
        bootstrap.addBundle(new AssetsBundle("/console/", "/"));
        bootstrap.addCommand(new InitializerCommand());
    }

    @Override
    public void run(FoxtrotServerConfiguration configuration, Environment environment) throws Exception {
        configuration.getHttpConfiguration().setRootPath("/foxtrot/*");

        // Create guice injector
        FoxtrotModule module = new FoxtrotModule(configuration, environment);
        Injector injector = Guice.createInjector(module);

        // Initialize managed objects
        environment.manage(injector.getInstance(HbaseTableConnection.class));
        environment.manage(injector.getInstance(ElasticsearchConnection.class));
        environment.manage(injector.getInstance(HazelcastConnection.class));
        environment.manage(injector.getInstance(TableMetadataManager.class));
        environment.manage(injector.getInstance(ManagedActionScanner.class));
        environment.manage(injector.getInstance(DataDeletionManager.class));
        environment.manage(injector.getInstance(ClusterManager.class));

        // Initialize resources
        addResources(injector, environment);

        List<HealthCheck> healthChecks = module.healthChecks(injector.getInstance(ElasticsearchConnection.class));
        healthChecks.forEach(environment::addHealthCheck);

        environment.addProvider(new FlatResponseTextProvider());
        environment.addProvider(new FlatResponseCsvProvider());
        environment.addProvider(new FlatResponseErrorTextProvider());

        environment.addFilter(CrossOriginFilter.class, "/*");
    }

    private void addManaged(Injector injector, Environment environment) {
        Reflections reflections = new Reflections("com.flipkart.foxtrot");
        Set<Class<? extends Managed>> managedClasses = reflections.getSubTypesOf(Managed.class);
        managedClasses.stream()
                .filter(managed -> !managed.isInterface() && !Modifier.isAbstract(managed.getModifiers()))
                .forEach(managed -> {
                    environment.manage(injector.getInstance(managed));
                });
    }

    private void addResources(Injector injector, Environment environment) {
        Reflections reflections = new Reflections("com.flipkart.foxtrot.server.resources");
        Set<Class<?>> resourceClasses = reflections.getTypesAnnotatedWith(Path.class);
        resourceClasses.stream()
                .filter(resource -> !resource.isInterface() && !Modifier.isAbstract(resource.getModifiers()))
                .forEach(resource -> {
                    environment.addResource(injector.getInstance(resource));
                });
    }

}
