package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.core.auth.FoxtrotRole;
import com.flipkart.foxtrot.core.auth.User;
import lombok.val;

import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 *
 */
public interface AuthStore {
    Optional<User> provision(final User user);

    Optional<User> getUser(final String userId);

    boolean deleteUser(final String id);

    boolean updateUser(final String id, UnaryOperator<User> mutator);

    default boolean grantRole(final String userId, final FoxtrotRole role) {
        return updateUser(userId, user -> {
            val roles = user.getRoles() == null ? new HashSet<FoxtrotRole>() : user.getRoles();
            roles.add(role);
            return new User(userId, roles, user.getTables(), user.getCreated(), new Date());
        });
    }

    default boolean revokeRole(final String userId, final FoxtrotRole role) {
        return updateUser(userId, user -> {
            val roles = user.getRoles() == null ? new HashSet<FoxtrotRole>() : user.getRoles();
            roles.remove(role);
            return new User(userId, roles, user.getTables(), user.getCreated(), new Date());
        });
    }

    default boolean grantTableAccess(final String userId, final String table) {
        return updateUser(userId, user -> {
            val tables = user.getTables() == null ? new HashSet<String>() : user.getTables();
            tables.add(table);
            return new User(userId, user.getRoles(), tables, user.getCreated(), new Date());
        });
    }

    default boolean revokeTableAccess(final String userId, final String table) {
        return updateUser(userId, user -> {
            val tables = user.getTables() == null ? new HashSet<String>() : user.getTables();
            tables.remove(table);
            return new User(userId, user.getRoles(), tables, user.getCreated(), new Date());
        });
    }

    Optional<Token> provisionToken(final String userId, TokenType tokenType, Date expiry);

    Optional<Token> getToken(final String tokenId);

    boolean deleteToken(final String tokenId);
}
