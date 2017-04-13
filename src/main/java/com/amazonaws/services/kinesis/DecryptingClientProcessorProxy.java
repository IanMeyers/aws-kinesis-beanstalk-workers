package com.amazonaws.services.kinesis;

import java.util.List;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.crypto.KinesisCryptoHelper;
import com.amazonaws.services.kinesis.model.Record;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DecryptingClientProcessorProxy extends ManagedClientProcessor {
	@Getter
	@NonNull
	private final ManagedClientProcessor processor;
	@Getter
	@NonNull
	private final KinesisCryptoHelper cryptoHelper;

	@Override
	public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
		try {
			// call the wrapped processor after decrypting the records using the
			// provided encryption config
			this.processor.processRecords(cryptoHelper.decryptRecords(records), checkpointer);
		} catch (Exception e) {
			// let the processor handle shutdown, in case it's been overridden
			this.processor.shutdown(checkpointer, ShutdownReason.TERMINATE);
		}
	}

	@Override
	public ManagedClientProcessor copy() throws Exception {
		return new DecryptingClientProcessorProxy(this.processor.copy(), this.cryptoHelper);
	}
}
