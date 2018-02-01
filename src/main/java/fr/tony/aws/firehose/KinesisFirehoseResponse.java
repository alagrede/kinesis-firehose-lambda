package fr.tony.aws.firehose;

import java.util.ArrayList;
import java.util.List;

public class KinesisFirehoseResponse {

	List<FirehoseRecord> records = new ArrayList<FirehoseRecord>();

	public List<FirehoseRecord> getRecords() {
		return records;
	}

	public void setRecords(List<FirehoseRecord> records) {
		this.records = records;
	}
	
}

