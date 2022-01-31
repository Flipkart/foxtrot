package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.core.auth.FoxtrotRole;
import com.flipkart.foxtrot.core.auth.User;
import io.dropwizard.util.Duration;

import java.util.Date;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 *
 */
public class IdmanAuthStore implements AuthStore {
    @Override
    public Optional<User> provisionUser(User user) {
        return Optional.empty();
    }

    @Override
    public Optional<User> getUser(String userId) {
        return Optional.empty();
    }

    @Override
    public boolean deleteUser(String userId) {
        return false;
    }

    @Override
    public boolean updateUser(
            String userId, UnaryOperator<User> mutator) {
        return false;
    }

    @Override
    public boolean grantRole(String userId, FoxtrotRole role) {
        return AuthStore.super.grantRole(userId, role);
    }

    @Override
    public boolean revokeRole(String userId, FoxtrotRole role) {
        return AuthStore.super.revokeRole(userId, role);
    }

    @Override
    public boolean grantTableAccess(String userId, String table) {
        return AuthStore.super.grantTableAccess(userId, table);
    }

    @Override
    public boolean revokeTableAccess(String userId, String table) {
        return AuthStore.super.revokeTableAccess(userId, table);
    }

    @Override
    public Optional<Token> provisionToken(String userId, TokenType tokenType, Date expiry) {
        return AuthStore.super.provisionToken(userId, tokenType, expiry);
    }

    @Override
    public Optional<Token> provisionToken(
            String userId, String tokenId, TokenType tokenType, Date expiry) {
        return Optional.empty();
    }

    @Override
    public Optional<Token> getToken(String tokenId) {
        return Optional.empty();
    }

    @Override
    public Optional<Token> getTokenForUser(String userId) {
        return Optional.empty();
    }

    @Override
    public boolean deleteToken(String tokenId) {
        return false;
    }

    @Override
    public boolean deleteExpiredTokens(Date date, Duration sessionDuration) {
        return false;
    }
}
