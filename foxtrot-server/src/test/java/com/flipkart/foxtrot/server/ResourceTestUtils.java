package com.flipkart.foxtrot.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.server.auth.DummyAuthFilter;
import com.flipkart.foxtrot.server.auth.UserPrincipal;
import com.flipkart.foxtrot.core.exception.provider.FoxtrotExceptionMapper;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;

/**
 *
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ResourceTestUtils {

    public static ResourceTestRule.Builder testResourceBuilder(ObjectMapper mapper) {
        return ResourceTestRule.builder()
                .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                .addProvider(new AuthDynamicFeature(new DummyAuthFilter()))
                .addProvider(RolesAllowedDynamicFeature.class)
                .addProvider(new AuthValueFactoryProvider.Binder<>(UserPrincipal.class))
                .setMapper(mapper)
                .addProvider(new FoxtrotExceptionMapper(mapper));
    }
}
