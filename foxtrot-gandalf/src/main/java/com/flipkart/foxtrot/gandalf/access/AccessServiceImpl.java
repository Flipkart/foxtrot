package com.flipkart.foxtrot.gandalf.access;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.TableActionRequestVisitor;
import com.flipkart.foxtrot.core.config.FoxtrotServerConfiguration;
import com.google.inject.Singleton;
import com.phonepe.gandalf.models.user.UserDetails;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import javax.validation.Valid;

/***
 Created by mudit.g on Apr, 2019
 ***/
@Data
@Singleton
public class AccessServiceImpl implements AccessService {

    private static final String SUPER_USER = "foxtrotSuperUser";
    private final boolean restrictAccess;
    private final ActionRequestVisitor<String> actionRequestVisitor;

    public AccessServiceImpl(FoxtrotServerConfiguration foxtrotConfiguration,
            ActionRequestVisitor<String> actionRequestVisitor) {
        this.restrictAccess = foxtrotConfiguration.isRestrictAccess();
        this.actionRequestVisitor = actionRequestVisitor;
    }

    @Override
    public boolean hasAccess(@Valid final ActionRequest request, UserDetails userDetails) {
        if (!restrictAccess) {
            return true;
        }
        String tableName = request.accept(actionRequestVisitor);
        return (userDetails.isAuthorized(SUPER_USER)) || (StringUtils.isEmpty(tableName) || userDetails.isAuthorized(
                tableName));
    }
}
