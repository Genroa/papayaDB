package papayaDB.api.queryParameters;

import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryType;

public class Type extends QueryParameter {
	public static void registerParameter() {
		for (QueryType type: QueryType.values()) {
			QueryParameter.parameter.get(type).put("type", new Type());
		}
		
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		json.put("get", value);
		return json;
	}

	public Stream<JsonObject> processQueryParameters(JsonObject parameters, Stream<JsonObject> elements) {
		//TODO;
		return null;
	}
}
