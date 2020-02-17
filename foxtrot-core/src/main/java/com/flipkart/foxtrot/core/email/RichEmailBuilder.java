package com.flipkart.foxtrot.core.email;

import com.flipkart.foxtrot.core.email.messageformatting.EmailBodyBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.EmailSubjectBuilder;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;

/**
 *
 */
@Singleton
public class RichEmailBuilder {
    private final EmailSubjectBuilder subjectBuilder;
    private final EmailBodyBuilder bodyBuilder;

    @Inject
    public RichEmailBuilder(
            EmailSubjectBuilder subjectBuilder,
            EmailBodyBuilder bodyBuilder) {
        this.subjectBuilder = subjectBuilder;
        this.bodyBuilder = bodyBuilder;
    }

    public final Email build(final String id, final List<String> recipients, final Map<String, Object> context) {
        if(null == subjectBuilder || null == bodyBuilder) {
            return Email.builder().build();
        }
        return new Email(subjectBuilder.content(id, context), bodyBuilder.content(id, context), recipients);
    }
}
