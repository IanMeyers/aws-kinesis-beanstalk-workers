package com.amazonaws.services.kinesis.crypto;

public class IncorrectEncryptionContextException extends Exception {
	public IncorrectEncryptionContextException() {
	}

	public IncorrectEncryptionContextException(String message) {
		super(message);
	}

	public IncorrectEncryptionContextException(Throwable cause) {
		super(cause);
	}

	public IncorrectEncryptionContextException(String message, Throwable cause) {
		super(message, cause);
	}

	public IncorrectEncryptionContextException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
