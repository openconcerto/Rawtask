package com.rawtask;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.logging.Level;

import org.eclipse.jetty.util.security.Credential;

public class PBKDF2Credential extends Credential {

	private static final long serialVersionUID = 4373433423318061392L;
	private final String hash;

	public PBKDF2Credential(String hash) {
		this.hash = hash;
	}

	@Override
	public boolean check(Object arg0) {
		try {
			return PasswordHash.validatePassword(arg0.toString(), this.hash);
		} catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
			TaskServer.LOGGER.log(Level.SEVERE, "cannot validate password", e);
			return false;
		}
	}

}
