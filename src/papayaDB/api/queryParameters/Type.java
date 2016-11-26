package papayaDB.api.queryParameters;

import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryType;

public class Type extends QueryParameter {
	public static void registerParameter() {
		for (QueryType type: QueryType.values()) {
			QueryParameter.parameter.get(type).put("type", new Type());
		}
		
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		json.put("type", value);
		return json;
	}
}
 