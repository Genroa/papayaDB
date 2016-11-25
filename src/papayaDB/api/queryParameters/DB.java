package papayaDB.api.queryParameters;

import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryType;

public class DB extends QueryParameter {
	public static void registerParameter() {
		for (QueryType type: QueryType.values()) {
			QueryParameter.parameter.get(type).put("db", new DB());
		}
		
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		json.put("db", value);
		return json;
	}

	public Stream<JsonObject> processQueryParameters(JsonObject parameters, Stream<JsonObject> elements) {
		//TODO;
		return null;
	}
}
 