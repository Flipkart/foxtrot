package com.flipkart.foxtrot.common.access;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.TableActionRequestVisitor;
import com.phonepe.gandalf.models.user.UserDetails;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import javax.validation.Valid;

/***
 Created by mudit.g on Apr, 2019
 ***/
@Data
public class AccessServiceImpl implements AccessService {
    private static final String SUPER_USER = "foxtrotSuperUser";
    private final boolean restrictAccess;
    private final TableActionRequestVisitor tableActionRequestVisitor;

    public AccessServiceImpl(boolean restrictAccess, TableActionRequestVisitor tableActionRequestVisitor) {
        this.restrictAccess = restrictAccess;
        this.tableActionRequestVisitor = tableActionRequestVisitor;
    }

    @Override
    public boolean hasAccess(@Valid final ActionRequest request, UserDetails userDetails) {
        if(! restrictAccess) {
            return true;
        }
        String tableName = request.accept(tableActionRequestVisitor);
        return (userDetails.isAuthorized(SUPER_USER)) || (StringUtils.isEmpty(tableName) || userDetails.isAuthorized(
                tableName));
    }
}
