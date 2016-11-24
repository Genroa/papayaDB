package papayaDB.api.queryParameters;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryType;
import papayaDB.db.Record;

public class Limit extends QueryParameter {

	public static void registerParameter() {
		System.out.println("Ask to Register Method");
		Map<String, Class<? extends QueryParameter>> param = new HashMap<String, Class<? extends QueryParameter>>();
		param.put("limit", Limit.class);
		QueryParameter.parameter.put(QueryType.GET, param);
	}

	public static JsonObject valueToJson(JsonObject json, String value) {
		JsonObject params = QueryParameter.getParams(json);
		json.put("parameters", params.put("limit", new JsonObject().put("value", Integer.parseInt(value))));
		return json;
	}

	public static Stream<JsonObject> processQueryParameters(JsonObject parameters, Stream<Record> elements) {
		long maxSize = parameters.getLong("value");
		return elements.limit(maxSize);
	}

}
