package papayaDB.api.queryParameters;

import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;
import papayaDB.api.query.SyntaxErrorException;

public class Order extends QueryParameter {
	public static void registerParameter() {
		QueryParameter.parameter.get(QueryType.GET).put("order", new Order());
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		JsonObject params = QueryParameter.getJsonParameters(json);
		value = value.substring(1, value.length() - 1);
		String[] values = value.split(";");
		if(values.length != 2) {
			throw new SyntaxErrorException("You must specify a field and an order (ASC or DESC)");
		}
		json.put("parameters", params.put("order", new JsonObject().put("field", values[0]).put("way", values[1])));
		return json;
	}
	
	public String valueToString(String key, JsonObject value) {
		StringBuilder sb = new StringBuilder(key).append("/[");
		sb.append(value.getString("field")).append(";").append(value.getString("way"));
		return sb.append("]").toString();
	}
}
 