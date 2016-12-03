package papayaDB.api.queryParameters;

import java.util.stream.Stream;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;

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
	
	public String valueToString(String key, JsonObject value) {
		StringBuilder sb = new StringBuilder(key).append("/[");
		value.getJsonArray("value");
		for (Object ja: value.getJsonArray("value")) { //On itère sur les éléments du json array (retourne un Object)
			JsonObject jo = ((JsonObject)ja);			//On transforme l'Objet en JsonObject
			sb	.append(jo.getString("field")).append(";")
				.append(jo.getValue("value").toString()).append(";");
		}
		return sb.deleteCharAt(sb.length() - 1).append("]").toString();
	}
}
 