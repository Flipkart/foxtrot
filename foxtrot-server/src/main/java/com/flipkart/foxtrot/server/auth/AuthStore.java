package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.core.auth.FoxtrotRole;
import com.flipkart.foxtrot.core.auth.User;
import io.dropwizard.util.Duration;
import lombok.SneakyThrows;
import lombok.val;

import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;

/**
 *
 */
public interface AuthStore {
    Optional<User> provisionUser(final User user);

    Optional<User> getUser(final String userId);

    boolean deleteUser(final String userId);

    boolean updateUser(final String userId, UnaryOperator<User> mutator);

    default boolean grantRole(final String userId, final FoxtrotRole role) {
        return updateUser(userId, user -> {
            val roles = user.getRoles() == null ? new HashSet<FoxtrotRole>() : user.getRoles();
            roles.add(role);
            return new User(userId, roles, user.getTables(), user.isSystemUser(), user.getCreated(), new Date());
        });
    }

    default boolean revokeRole(final String userId, final FoxtrotRole role) {
        return updateUser(userId, user -> {
            val roles = user.getRoles() == null ? new HashSet<FoxtrotRole>() : user.getRoles();
            roles.remove(role);
            return new User(userId, roles, user.getTables(), user.isSystemUser(), user.getCreated(), new Date());
        });
    }

    default boolean grantTableAccess(final String userId, final String table) {
        return updateUser(userId, user -> {
            val tables = user.getTables() == null ? new HashSet<String>() : user.getTables();
            tables.add(table);
            return new User(userId, user.getRoles(), tables, user.isSystemUser(), user.getCreated(), new Date());
        });
    }

    default boolean revokeTableAccess(final String userId, final String table) {
        return updateUser(userId, user -> {
            val tables = user.getTables() == null ? new HashSet<String>() : user.getTables();
            tables.remove(table);
            return new User(userId, user.getRoles(), tables, user.isSystemUser(), user.getCreated(), new Date());
        });
    }

    default Optional<Token> provisionToken(final String userId, TokenType tokenType, Date expiry) {
        return provisionToken(userId, UUID.randomUUID().toString(), tokenType, expiry);
    }

    Optional<Token> provisionToken(final String userId, String tokenId, TokenType tokenType, Date expiry);

    Optional<Token> getToken(final String tokenId);

    @SneakyThrows
    Optional<Token> getTokenForUser(String userId);

    boolean deleteToken(final String tokenId);

    boolean deleteExpiredTokens(Date date, Duration sessionDuration);
}
