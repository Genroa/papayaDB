package papayaDB.api.queryParameters;

import java.util.stream.Stream;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryType;

public class Equals extends QueryParameter {
	
	public static void registerParameter() {
		QueryParameter.parameter.get(QueryType.GET).put("equals", new Equals());
		QueryParameter.parameter.get(QueryType.UPDATE).put("equals", new Equals());
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		JsonObject params = QueryParameter.getJsonParameters(json);
		value = value.substring(1, value.length() - 1);
		JsonArray equals = new JsonArray();
		for (String s: value.split(";")) {
			JsonObject equal = new JsonObject();
			String[] tmp = s.split("=");
			equal	.put("field", tmp[0])
					.put("value", tmp[1].substring(3, tmp[1].length() - 3));
			equals.add(equal);
		}
		json.put("parameters", params.put("equals", new JsonObject().put("value", equals)));
		return json;
	}

	public Stream<JsonObject> processQueryParameters(JsonObject parameters, Stream<JsonObject> elements) {
		//TODO;
		return null;
	}
}
 