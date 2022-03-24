package com.flipkart.foxtrot.core.email;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;

import javax.mail.*;
import javax.mail.internet.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

/***
 Created by nitish.goyal on 06/10/18
 ***/
@Slf4j
public class EmailClient {

    private final EmailConfig emailConfig;
    private final Session session;

    public EmailClient(EmailConfig emailConfig) {
        this.emailConfig = emailConfig;
        Properties mailProps = new Properties();
        mailProps.put("mail.transport.protocol", "smtp");
        mailProps.put("mail.smtp.port", emailConfig.getPort());
        mailProps.put("mail.smtp.auth", false);
        mailProps.put("mail.smtp.host", emailConfig.getHost());
        mailProps.put("mail.smtp.startttls.enable", false);
        mailProps.put("mail.smtp.timeout", 10000);
        mailProps.put("mail.smtp.connectiontimeout", 10000);
        this.session = Session.getDefaultInstance(mailProps);
    }

    public void sendEmail(final Email email) {
        if (Strings.isNullOrEmpty(emailConfig.getFrom())) {
            log.warn("Mail config not set properly. No mail will be sent.");
            return;
        }
        try {
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(emailConfig.getFrom()));
            final List<String> recipients = recipients(email);
            if (recipients.isEmpty()) {
                return;
            }
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(String.join(",", recipients)));
            message.setSubject(email.getSubject());

            InternetHeaders headers = new InternetHeaders();
            headers.addHeader("Content-type", "text/html; charset=UTF-8");

            final String content = email.getContent();
            if (null != content) {
                BodyPart messageBodyPart = new MimeBodyPart(headers, content.getBytes(StandardCharsets.UTF_8));
                Multipart multipart = new MimeMultipart();
                multipart.addBodyPart(messageBodyPart);
                message.setContent(multipart);
            }
            log.debug("Sending email with subject: {}, message: {}, to receipients : {} ", email.getSubject(),
                    email.getContent(), email.getRecipients());
            Transport.send(message, emailConfig.getUser(), emailConfig.getPassword());
        } catch (Exception e) {
            log.error("Error occurred while sending the email :{}", email, e);
        }
    }

    private List<String> recipients(Email email) {
        final List<String> emailRecipients = email.getRecipients();
        final List<String> defaultRecipients = emailConfig.getEventNotificationEmails();
        final ImmutableList.Builder<String> recipientsBuilder = ImmutableList.builder();
        if (null != email.getRecipients()) {
            recipientsBuilder.addAll(emailRecipients);
        }
        if (null != defaultRecipients) {
            recipientsBuilder.addAll(defaultRecipients);
        }
        return recipientsBuilder.build();
    }
}
