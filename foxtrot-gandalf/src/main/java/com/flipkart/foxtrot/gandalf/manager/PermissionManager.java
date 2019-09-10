package com.flipkart.foxtrot.gandalf.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.TableV2;
import com.flipkart.foxtrot.gandalf.exception.*;
import com.phonepe.gandalf.models.authn.UserGroupNamespace;
import com.phonepe.gandalf.models.authn.requests.LoginRequest;
import com.phonepe.gandalf.models.authn.requests.PasswordLoginRequest;
import com.phonepe.gandalf.models.authn.requests.TTLInfo;
import com.phonepe.gandalf.models.authn.requests.UserPermissionRequest;
import com.phonepe.gandalf.models.authn.response.LoginResponse;
import com.phonepe.gandalf.models.authz.Permission;
import com.phonepe.gandalf.models.authz.PermissionRequest;
import com.phonepe.gandalf.models.user.User;
import com.phonepe.platform.http.Endpoint;
import com.phonepe.platform.http.OkHttpUtils;
import com.phonepe.platform.http.ServiceEndpointProvider;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.util.Optional;
import java.util.Set;

@Slf4j
public class PermissionManager {

    private final OkHttpClient okHttp;
    private final Endpoint endpoint;
    private final ObjectMapper mapper;
    private static final String AUTHORIZATION = "Authorization";
    private static final String ECHO = "echo";
    private static final MediaType APPLICATION_JSON = MediaType.parse("application/json");

    public PermissionManager(ObjectMapper mapper, OkHttpClient okHttp, ServiceEndpointProvider endpointProvider) {
        this.mapper = mapper;
        this.endpoint = endpoint(endpointProvider);
        this.okHttp = okHttp;
    }

    public void manage(TableV2 table) {
        String permissionId;
        Request request;
        RequestBody body;
        okhttp3.Response response;
        byte[] responseBody;

        String authToken = getAuthToken(ECHO);

        final HttpUrl url = endpoint.url("/v1/permissions");
        PermissionRequest permissionRequest = new PermissionRequest(table.getName(), true);

        try {
            body = RequestBody.create(APPLICATION_JSON,
                    mapper.writeValueAsString(permissionRequest));
            request = new Request.Builder()
                    .url(url)
                    .header(AUTHORIZATION, String.format("Bearer %s", authToken))
                    .post(body)
                    .build();

            response = okHttp.newCall(request).execute();
            responseBody = OkHttpUtils.body(response);
            permissionId = mapper.readValue(responseBody, Permission.class).getPermissionId();
        } catch(Exception e) {
            throw new PermissionCreationException("Not able to create new permission", e);
        }

        if (!response.isSuccessful() || null == responseBody) {
            log.error("Error in creating permission Post Call response {}", response);
            throw new PermissionCreationException("Exception while creating new permission");
        }

        String[] emailIds = table.getAdminEmails().replaceAll("\\s+", "").split(",");
        for(String emailId: emailIds) {
            createUserPermission(permissionId, emailId, authToken);
        }
    }

    private String getAuthToken(String namespace) {
        final HttpUrl url = endpoint.url("/v1/auth/login");
        RequestBody requestBody;
        okhttp3.Response response;
        byte[] responseBody;

        LoginRequest loginRequest = new PasswordLoginRequest("mudit.g@phonepe.com", "gandalf@echo",
                            TTLInfo.builder()
                                    .jwtTtlSeconds(10)
                                    .ttlSeconds(10)
                                    .build());
        try {
            requestBody = RequestBody.create(APPLICATION_JSON,
                    mapper.writeValueAsBytes(loginRequest));
            Request request = new Request.Builder()
                    .url(url)
                    .header("NAMESPACE", namespace)
                    .post(requestBody)
                    .build();
            response = okHttp.newCall(request).execute();
            responseBody = OkHttpUtils.body(response);

            LoginResponse loginResponse = mapper.readValue(responseBody, LoginResponse.class);
            return loginResponse.getToken();
        } catch(Exception e) {
            throw new AuthTokenException("Not able to generate auth token", e);
        }
    }

    private void createUserPermission(String permissionId, String emailId, String authToken) {
        RequestBody requestBody;
        okhttp3.Response response;

        User user = getUser(emailId, authToken);
        if (user == null) {
            throw new UserNotFoundException("Not able to retrieve user details with email : " + emailId);
        }

        final HttpUrl url = endpoint.url(String.format("/v1/user/%s/permission/%s", user.getUserId(), permissionId);
        Set<UserGroupNamespace> userGroupNamespaces = user.getUserGroupNamespaces();
        Long userGroupId = 0L;
        int userGroupIdFlag = 0;

        for(UserGroupNamespace userGroupNamespace : userGroupNamespaces) {
            if (userGroupNamespace.getNamespace().equals(ECHO)) {
                userGroupId = userGroupNamespace
                        .getUserRole()
                        .getUserGroupId();
                userGroupIdFlag = 1;
                break;
            }
        }
        if (userGroupIdFlag == 0) {
            throw new UserPermissionAdditionException("Not able to find User group ID for the given user");
        }
        try {
            requestBody = RequestBody.create(APPLICATION_JSON,
                    mapper.writeValueAsBytes(
                            UserPermissionRequest
                                    .builder()
                                    .userGroupId(userGroupId)
                                    .build()));
            Request request = new Request.Builder()
                    .url(url)
                    .header(AUTHORIZATION, String.format("Bearer %s", authToken))
                    .post(requestBody)
                    .build();
            response = okHttp.newCall(request).execute();
        } catch(Exception e) {
            throw new UserPermissionAdditionException("Not able to add new permission to user", e);
        }

        if (!response.isSuccessful()) {
            log.error("Error in creating user permission Post Call response {}", response);
            throw new UserPermissionAdditionException("Exception while adding permission to user");
        }
    }

    private User getUser(String emailId, String authToken) {
        okhttp3.Response response;
        byte[] responseBody;

        final HttpUrl url = endpoint.url(String.format("/v1/user/?email=%s", emailId));
        Request request = new Request.Builder()
                .url(url)
                .header(AUTHORIZATION, String.format("Bearer %s", authToken))
                .get()
                .build();
        try {
            response = okHttp.newCall(request).execute();
            responseBody = OkHttpUtils.body(response);
            if (!response.isSuccessful() || null == responseBody) {
                log.error("Error in retrieving user details get Call response {}", response);
                return null;
            }
            return mapper.readValue(responseBody, User.class);
        } catch(Exception e) {
            throw new UserNotFoundException("Not able to retrieve user details", e);
        }
    }

    private Endpoint endpoint(ServiceEndpointProvider endpointProvider) {
        Optional<Endpoint> endpointOptional = endpointProvider.endpoint();
        if (!endpointOptional.isPresent()) {
            throw new EndpointNotFoundException("No endpoint found for Gandalf http service");
        }
        return endpointOptional.get();
    }
}
