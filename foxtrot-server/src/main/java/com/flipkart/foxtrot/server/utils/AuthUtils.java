package com.flipkart.foxtrot.server.utils;

import com.flipkart.foxtrot.server.auth.AuthConfig;
import com.flipkart.foxtrot.server.auth.JwtConfig;
import com.flipkart.foxtrot.server.auth.Token;
import io.dropwizard.util.Duration;
import java.nio.charset.StandardCharsets;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.NumericDate;
import org.jose4j.keys.HmacKey;

/**
 *
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AuthUtils {

    @SneakyThrows
    public static String createJWT(final Token token,
                                   final JwtConfig jwtConfig) {
        JwtClaims claims = new JwtClaims();
        claims.setIssuer(jwtConfig.getIssuerId());
        claims.setGeneratedJwtId();
        claims.setIssuedAtToNow();
        claims.setJwtId(token.getId());
        claims.setNotBeforeMinutesInThePast(2);
        claims.setSubject(token.getUserId());
        claims.setAudience(token.getTokenType()
                .name());
        if (null != token.getExpiry()) {
            claims.setExpirationTime(NumericDate.fromMilliseconds(token.getExpiry()
                    .getTime()));
        }
        JsonWebSignature jws = new JsonWebSignature();
        jws.setPayload(claims.toJson());
        final byte[] secretKey = jwtConfig.getPrivateKey()
                .getBytes(StandardCharsets.UTF_8);
        jws.setKey(new HmacKey(secretKey));
        jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.HMAC_SHA512);
        return jws.getCompactSerialization();
    }

    public static Duration sessionDuration(AuthConfig authConfig) {
        final Duration dynamicSessionDuration = authConfig.getJwt()
                .getSessionDuration();
        return dynamicSessionDuration != null
               ? dynamicSessionDuration
               : Duration.days(30);
    }
}
