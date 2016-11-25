package papayaDB.api.queryParameters;

import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryType;

public class Limit extends QueryParameter {
	public static void registerParameter() {
		System.out.println("Ask to Register Method");
		QueryParameter.parameter.get(QueryType.GET).put("limit", new Limit());
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		JsonObject params = QueryParameter.getJsonParameters(json);
		System.out.println(value);
		json.put("parameters", params.put("limit", new JsonObject().put("value", Integer.parseInt(value))));
		System.out.println("valueToJson = " + json);
		return json;
	}

	public Stream<JsonObject> processQueryParameters(JsonObject parameters, Stream<JsonObject> elements) {
		long maxSize = parameters.getLong("value");
		return elements.limit(maxSize);
	}
}
 