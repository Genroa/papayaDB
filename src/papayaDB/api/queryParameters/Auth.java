package papayaDB.api.queryParameters;

import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;
import papayaDB.api.query.SyntaxErrorException;

public class Auth extends QueryParameter {
	public static void registerParameter() {
		QueryParameter.parameter.get(QueryType.CREATEDB).put("auth", new Auth());
		QueryParameter.parameter.get(QueryType.DELETE).put("auth", new Auth());
		QueryParameter.parameter.get(QueryType.DELETEDB).put("auth", new Auth());
		QueryParameter.parameter.get(QueryType.INSERT).put("auth", new Auth());
		QueryParameter.parameter.get(QueryType.UPDATE).put("auth", new Auth());
	}
	
	public JsonObject valueToJson(JsonObject json, String value) {
		value = value.substring(1, value.length() - 1); // on retire les [ ]
		String[] values = value.split(";"); //on découpe au ;
		if(values.length != 2) { //Si il n'y a pas deux params c'est pas bon
			throw new SyntaxErrorException("username and password must be specified");
		}
		json.put("auth", new JsonObject().put("user", values[0]).put("password", values[1])); // on ajoute le champ auth au json
		return json;
	}
	
	public String valueToString(String key, JsonObject value) {
		StringBuilder sb = new StringBuilder(key).append("/[");
		sb	.append(value.getString("user")).append(";")
			.append(value.getString("pass"));
		return sb.append("]").toString();
	}
}
