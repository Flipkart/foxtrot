package com.flipkart.foxtrot.core.email;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.InternetHeaders;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 Created by nitish.goyal on 06/10/18
 ***/
@Singleton
public class EmailClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmailClient.class);

    private final EmailConfig emailConfig;
    private final Session session;

    @Inject
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

    public boolean sendEmail(final Email email) {
        if (Strings.isNullOrEmpty(emailConfig.getFrom())) {
            LOGGER.warn("Mail config not set properly. No mail will be sent.");
            return false;
        }
        try {
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(emailConfig.getFrom()));
            final List<String> recipients = recipients(email);
            if (recipients.isEmpty()) {
                return false;
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
            Transport.send(message, emailConfig.getUser(), emailConfig.getPassword());
        } catch (Exception e) {
            LOGGER.error("Error occurred while sending the email :%s", e);
            return false;
        }
        return true;

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
