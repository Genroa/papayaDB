package papayaDB.api.queryParameters;

import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;

public class Order extends QueryParameter {
	public static void registerParameter() {
		QueryParameter.parameter.get(QueryType.GET).put("order", new Order());
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		JsonObject params = QueryParameter.getJsonParameters(json);
		value = value.substring(1, value.length() - 1);
		String[] values = value.split(";");
		if(values.length != 2) {
			//TODO: exception order;
		}
		json.put("parameters", params.put("order", new JsonObject().put("field", values[0]).put("way", values[1])));
		return json;
	}
}
 