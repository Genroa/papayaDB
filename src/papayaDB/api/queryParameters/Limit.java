package papayaDB.api.queryParameters;

import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;
import papayaDB.api.query.SyntaxErrorException;
import papayaDB.db.FileStorageManager;

public class Limit extends QueryParameter {
	
	public Limit() {
		isTerminalModifier = true;
	}
	
	public static void registerParameter() {
		System.out.println("Ask to Register Method");
		QueryParameter.parameter.get(QueryType.GET).put("limit", new Limit());
	}

	public JsonObject valueToJson(JsonObject json, String value) throws SyntaxErrorException {
		JsonObject params = QueryParameter.getJsonParameters(json);
		int intValue;
		try {
			intValue = Integer.parseInt(value);
		} catch (NumberFormatException e) {
			throw new SyntaxErrorException("limit parameter must be a number");
		}
		json.put("parameters", params.put("limit", new JsonObject().put("value", intValue)));
		System.out.println("valueToJson = " + json);
		return json;
	}
	
	@Override
	public Stream<JsonObject> processTerminalOperation(JsonObject parameters, Stream<JsonObject> elements, FileStorageManager storageManager) {
		long maxSize = parameters.getLong("value");
		return elements.limit(maxSize);
	}
	
	public String valueToString(String key, JsonObject value) {
		StringBuilder sb = new StringBuilder(key + "/");
		sb.append(value.getInteger("value"));
		return sb.toString();
	}
}
 