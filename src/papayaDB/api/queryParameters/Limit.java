package papayaDB.api.queryParameters;

import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;
import papayaDB.api.query.SyntaxErrorException;

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

	public Stream<JsonObject> processQueryParameters(JsonObject parameters, Stream<JsonObject> elements) {
		long maxSize = parameters.getLong("value");
		return elements.limit(maxSize);
	}
}
 