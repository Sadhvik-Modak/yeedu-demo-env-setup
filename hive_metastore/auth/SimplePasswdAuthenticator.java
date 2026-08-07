package com.yeedu.demo.auth;

import javax.security.sasl.AuthenticationException;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;

/**
 * Minimal HiveServer2 CUSTOM authenticator: accepts one user/password pair
 * read from HIVE_USER / HIVE_PASSWORD environment variables at connect time.
 */
public class SimplePasswdAuthenticator implements PasswdAuthenticationProvider {

    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
        String expectedUser = System.getenv("HIVE_USER");
        String expectedPassword = System.getenv("HIVE_PASSWORD");

        if (expectedUser == null || expectedPassword == null) {
            throw new AuthenticationException("HIVE_USER/HIVE_PASSWORD not configured on server");
        }
        if (!expectedUser.equals(user) || !expectedPassword.equals(password)) {
            throw new AuthenticationException("Invalid credentials for user " + user);
        }
    }
}
