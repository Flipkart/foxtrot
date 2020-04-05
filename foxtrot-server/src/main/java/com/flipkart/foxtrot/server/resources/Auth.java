package com.flipkart.foxtrot.server.resources;

import com.flipkart.foxtrot.core.auth.FoxtrotRole;
import com.flipkart.foxtrot.core.auth.User;
import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.AuthStore;
import com.flipkart.foxtrot.server.auth.TokenType;
import com.flipkart.foxtrot.server.auth.io.CreateUserRequest;
import com.flipkart.foxtrot.server.utils.AuthUtils;
import io.swagger.annotations.Api;
import lombok.val;
import org.hibernate.validator.constraints.NotEmpty;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Date;

/**
 *
 */
@Path("/v1/auth")
@Produces(MediaType.APPLICATION_JSON)
@Api("Auth related APIs. DO NOT expose these for public access.")
public class Auth {
    private final Provider<AuthStore> authProvider;
    private final AuthConfig authConfig;

    @Inject
    public Auth(Provider<AuthStore> authProvider, AuthConfig authConfig) {
        this.authProvider = authProvider;
        this.authConfig = authConfig;
    }

    @POST
    @Path("/users")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response provisionUser(@NotNull @Valid final CreateUserRequest createUserRequest) {
        val user = new User(createUserRequest.getId(), createUserRequest.getRoles(), createUserRequest.getTables(),
                            new Date(), new Date());
        return Response.ok(authProvider.get().provision(user)).build();
    }

    @GET
    @Path("/users/{userId}")
    public Response getUser(@NotNull @NotEmpty @PathParam("userId") final String userId) {
        return Response.ok(authProvider.get().getUser(userId)).build();
    }

    @PUT
    @Path("/users/{userId}/roles/grant/{role}")
    public Response grantRole(@NotNull @NotEmpty @PathParam("userId") final String userId,
                                    @NotNull @PathParam("role") final FoxtrotRole role) {
        val status = authProvider.get()
                .grantRole(userId, role);
        return updateUserResponse(userId, status);
    }

    @PUT
    @Path("/users/{userId}/roles/revoke/{role}")
    public Response revokeRole(@NotNull @NotEmpty @PathParam("userId") final String userId,
                                    @NotNull @PathParam("role") final FoxtrotRole role) {
        val status = authProvider.get()
                .revokeRole(userId, role);
        return updateUserResponse(userId, status);
    }

    @PUT
    @Path("/users/{userId}/tables/access/grant/{table}")
    public Response grantTableAccess(@NotNull @NotEmpty @PathParam("userId") final String userId,
                                    @NotNull @NotEmpty @PathParam("table") final String table) {
        val status = authProvider.get()
                .grantTableAccess(userId, table);
        return updateUserResponse(userId, status);
    }

    @PUT
    @Path("/users/{userId}/tables/access/revoke/{table}")
    public Response revokeTableAccess(@NotNull @NotEmpty @PathParam("userId") final String userId,
                                      @NotNull @NotEmpty @PathParam("table") final String table) {
        val status = authProvider.get()
                .revokeTableAccess(userId, table);
        return updateUserResponse(userId, status);
    }

    @DELETE
    @Path("/users/{userId}")
    public Response deleteUser(@NotNull @NotEmpty @PathParam("userId") final String userId) {
        final boolean status = authProvider.get().deleteUser(userId);
        if(!status) {
            return Response.notModified().build();
        }
        return Response.ok().build();
    }

    @POST
    @Path("/tokens/{userId}")
    public Response provisionToken(@NotNull @NotEmpty @PathParam("userId") final String userId) {
        val token = authProvider.get().provisionToken(userId, TokenType.STATIC, null).orElse(null);
        if(null == token) {
            return Response.notModified().build();
        }
        return Response
                .ok(Collections.singletonMap("jwt", AuthUtils.createJWT(token, authConfig.getJwt())))
                .build();
    }

    @GET
    @Path("/tokens/{tokenId}")
    public Response getToken(@NotNull @NotEmpty @PathParam("tokenId") final String tokenId) {
        return Response.ok(authProvider.get().getToken(tokenId))
                .build();
    }

    @DELETE
    @Path("/tokens/{userId}")
    public Response deleteToken(@NotNull @NotEmpty @PathParam("userId") final String userId,
                                   @NotNull @NotEmpty @PathParam("tokenId") final String tokenId) {
        val status = authProvider.get().deleteToken(tokenId);
        if(!status) {
            return Response.notModified().build();
        }
        return Response.ok().build();
    }

    private Response updateUserResponse(String userId, boolean status) {
        if (!status) {
            return Response.notModified()
                    .build();
        }
        return Response.ok()
                .entity(authProvider.get().getUser(userId))
                .build();
    }
}
