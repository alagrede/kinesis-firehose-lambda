package fr.tony.aws;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisFirehoseEvent;
import com.google.gson.JsonObject;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import fr.tony.aws.firehose.FirehoseRecord;
import fr.tonycop.aws.firehose.KinesisFirehoseResponse;

public class ProcessKinesisEvents implements RequestHandler<KinesisFirehoseEvent, KinesisFirehoseResponse> {

	private Schema schema = new Parser().parse(getSchema());
	
	@Override
	public KinesisFirehoseResponse handleRequest(KinesisFirehoseEvent event, Context context) {
		LambdaLogger logger = context.getLogger();
        
		logger.log("Lambda started. Got messages " + event.getRecords().size());

        KinesisFirehoseResponse response = new KinesisFirehoseResponse();
        
        for (KinesisFirehoseEvent.Record record : event.getRecords()) {

        	// Kinesis data are Base64 encoded
        	byte[] data = record.getData().array();
        	
        	// Issue: the value is contained in ""
        	String strBase64Msg = new String(data, StandardCharsets.UTF_8).replaceAll("\"", "");
        	
        	logger.log("data:" + strBase64Msg);
        	data = Base64.getDecoder().decode(strBase64Msg);
        	
            String decodedMessage = decodeAvroMessage(data, logger);
            
            logger.log("Decoded message: " + decodedMessage);
            
            FirehoseRecord transformedRecord = new FirehoseRecord();
            transformedRecord.setRecordId(record.getRecordId());
            transformedRecord.setResult(FirehoseRecord.ResultState.TRANSFORMED_STATE_OK.getState());
            transformedRecord.encodeAndSetData(decodedMessage);

            response.getRecords().add(transformedRecord);
        }
        
        return response;
	}

	private String decodeAvroMessage(byte[] srcData, LambdaLogger logger) {
		
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		
		byte[] encodedline = srcData;
		GenericRecord record = recordInjection.invert(encodedline).get();
		List<Field> fields = schema.getFields();
		List<String> schemaFields = fields.stream().map(field -> field.name()).collect(Collectors.toList());

		//String header = String.join(";", schemaFields);
		
		// Convert decoded message to JSON
		JsonObject json = new JsonObject();

		schemaFields.stream().forEach(field -> json.addProperty(field, record.get(field).toString()) );
		
		if (logger != null)
			logger.log("decoded message: " + json.toString());
		
		return json.toString();
	}

	private String getSchema() {
		try {
			ClassLoader classLoader = getClass().getClassLoader();
			File myfile = new File(classLoader.getResource("schema.json").getFile());
			return new String(Files.readAllBytes(myfile.toPath()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}

}