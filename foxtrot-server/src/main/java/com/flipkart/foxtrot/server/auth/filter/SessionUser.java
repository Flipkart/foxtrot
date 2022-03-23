package com.flipkart.foxtrot.server.auth.filter;

import com.flipkart.foxtrot.server.auth.UserPrincipal;
import lombok.Builder;
import lombok.Data;
import lombok.val;

import java.io.Serializable;

/**
 *
 */
@Data
@Builder
public class SessionUser implements Serializable {
    private static final long serialVersionUID = -7917711435258380077L;
    private static ThreadLocal<UserPrincipal> currentUser = new ThreadLocal<>();
    private final UserPrincipal user;

    public static void put(UserPrincipal user) {
        currentUser.set(user);
    }

    public static UserPrincipal take() {
        val user = currentUser.get();
        currentUser.remove();
        return user;
    }

}
