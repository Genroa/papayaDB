package papayaDB.api.queryParameters;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;

public class Bounds extends QueryParameter {
	public static void registerParameter() {
		QueryParameter.parameter.get(QueryType.GET).put("bounds", new Bounds());
		QueryParameter.parameter.get(QueryType.UPDATE).put("bounds", new Bounds());
	}

	public JsonObject valueToJson(JsonObject json, String value) {
		JsonObject params = QueryParameter.getJsonParameters(json);
		value = value.substring(1, value.length() - 1);
		String[] values = value.split(";");
		if(values.length % 3 != 0) {
			//TODO : exception bounds.
		}
		JsonArray bounds = new JsonArray();
		for (int i = 0; i < values.length; i += 3) {
			JsonObject bound = new JsonObject();
			bound	.put("field", values[i])
					.put("min", values[i + 1])
					.put("max", values[i + 2]);
			bounds.add(bound);
		}
		json.put("parameters", params.put("bounds", new JsonObject().put("value", bounds)));
		return json;
	}
	
	public String valueToString(String key, JsonObject value) {
		StringBuilder sb = new StringBuilder(key).append("/[");
		for (Object ja: value.getJsonArray("value")) { //On itère sur les éléments du json array (retourne un Object)
			JsonObject jo = ((JsonObject)ja);			//On transforme l'Objet en JsonObject
			sb	.append(jo.getString("field")).append(";")
				.append(jo.getInteger("min").toString()).append(";")
				.append(jo.getInteger("max").toString()).append(";");
		}
		return sb.deleteCharAt(sb.length() - 1).append("]").toString();
	}
}
 