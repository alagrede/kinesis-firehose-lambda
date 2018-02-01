package fr.tony.aws.firehose;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class FirehoseRecord {

    /**
     *The record ID is passed from Firehose to Lambda during the invocation. The transformed record must
     *contain the same record ID. Any mismatch between the ID of the original record and the ID of the
     *transformed record is treated as a data transformation failure.
     **/
	String recordId = "";
	
	  /**
     * The status of the data transformation of the record. The possible values are: "Ok"
     * (the record was transformed successfully), "Dropped" (the record was dropped intentionally
     * by your processing logic), and "ProcessingFailed" (the record could not be transformed).
     * If a record has a status of "Ok" or "Dropped", Firehose considers it successfully
     * processed. Otherwise, Firehose considers it unsuccessfully processed.
     *
     * Possible values:
     * * Ok - The record was transformed successfully
     * * Dropped- The record was dropped intentionally by your processing logic
     * * ProcessingFailed - The record could not be transformed
     **/
	String result = "";

    /**
    * The transformed data payload, after base64-encoding.
    **/
	String  data = "";


	public String getRecordId() {
		return recordId;
	}

	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public void encodeAndSetData(String unencodedData) {
		this.data = Base64.getEncoder().encodeToString(unencodedData.getBytes(StandardCharsets.UTF_8));
	}

	public void encodeAndSetData(byte[] unencodedData) {
		this.data = Base64.getEncoder().encodeToString(unencodedData);
	}
	

	public static enum ResultState {

		TRANSFORMED_STATE_OK("Ok"),
		TRANSFORMED_STATE_DROPPED("Dropped"),
		TRANSFORMED_STATE_PROCESSINGFAILED("ProcessingFailed");

		private String state;
		ResultState(String state) {
			this.state = state;
		}
		
		public String getState() {
			return state;
		}

	}
}


