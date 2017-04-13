package com.amazonaws.services.kinesis.crypto;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.amazonaws.encryptionsdk.kms.KmsMasterKey;
import com.amazonaws.encryptionsdk.kms.KmsMasterKeyProvider;
import com.amazonaws.services.kinesis.model.Record;

import lombok.Getter;
import lombok.NonNull;

public class KinesisCryptoHelper {
	@Getter
	private KmsMasterKeyProvider kmsProvider;

	@Getter
	@NonNull
	private final String keyArn;

	@Getter
	private final Map<String, String> encryptionContext;

	private boolean validateEncryptionContext = true;
	private final AwsCrypto crypto = new AwsCrypto();
	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

	public KinesisCryptoHelper(String keyArn, Map<String, String> encryptionContext) {
		this.keyArn = keyArn;
		this.kmsProvider = new KmsMasterKeyProvider(this.keyArn);
		this.encryptionContext = encryptionContext;
	}

	/**
	 * Encrypt a single string value with the provided KMS Key and Encryption
	 * Context
	 * 
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public String encryptValue(String value) throws Exception {
		return crypto.encryptString(this.kmsProvider, value, this.encryptionContext).getResult();
	}

	/**
	 * Encrypt a list of Kinesis Records using the provided KMS Key and
	 * Encryption Context
	 * 
	 * @param records
	 * @return
	 * @throws Exception
	 */
	public List<Record> encryptRecords(List<Record> records) throws Exception {
		List<Record> outputRecords = new ArrayList<>(records.size());

		for (Record r : records) {
			String c = encryptValue(new String(r.getData().array()));
			outputRecords.add(r.withData(ByteBuffer.wrap(c.getBytes())));
		}

		return outputRecords;
	}

	/**
	 * Decrypt a String value using the provided KMS Key and Encryption Context
	 * 
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public String decryptValue(String value)
			throws InvalidKeyException, IncorrectEncryptionContextException, CharacterCodingException {
		CryptoResult<String, KmsMasterKey> decryptedResult = crypto.decryptString(this.kmsProvider,
				decoder.decode(ByteBuffer.wrap(value.getBytes())).toString());

		// validate the keyArn embedded into the value
		if (decryptedResult.getMasterKeyIds().size() > 0
				|| !decryptedResult.getMasterKeyIds().get(0).equals(this.keyArn)) {
			throw new InvalidKeyException(
					String.format("Supplied Key ARN %s not used to encrypt recieved data", this.keyArn));
		}

		// Validate encryption context
		// For more information about encryption context, see
		// https://amzn.to/1nSbe9X (blogs.aws.amazon.com)
		if (this.encryptionContext != null && this.validateEncryptionContext) {
			for (String k : this.encryptionContext.keySet()) {
				if (!decryptedResult.getEncryptionContext().get(k).equals(this.encryptionContext.get(k))) {
					throw new IncorrectEncryptionContextException();
				}
			}
		}

		return decryptedResult.getResult();
	}

	/**
	 * Decrypt a list of Kinesis Records using the KMS Key and Encryption
	 * Context
	 * 
	 * @param records
	 * @return
	 * @throws Exception
	 */
	public List<Record> decryptRecords(List<Record> records)
			throws InvalidKeyException, IncorrectEncryptionContextException, CharacterCodingException {
		List<Record> outputRecords = new ArrayList<>(records.size());
		for (Record r : records) {
			String decrypted = decryptValue(new String(r.getData().array()));

			outputRecords.add(r.withData(ByteBuffer.wrap(decrypted.getBytes())));
		}

		return outputRecords;
	}

	public KinesisCryptoHelper withKmsProvider(KmsMasterKeyProvider kmsProvider) {
		this.kmsProvider = kmsProvider;
		return this;
	}

	public KinesisCryptoHelper withValidateEncryptionContext(boolean validate) {
		this.validateEncryptionContext = validate;
		return this;
	}
}
