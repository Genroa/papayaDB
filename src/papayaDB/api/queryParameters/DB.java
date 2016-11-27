package papayaDB.api.queryParameters;

import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryType;
import papayaDB.api.query.SyntaxErrorException;

public class DB extends QueryParameter {
	public static void registerParameter() {
		for (QueryType type: QueryType.values()) {
			QueryParameter.parameter.get(type).put("db", new DB());
		}
		
	}

	public JsonObject valueToJson(JsonObject json, String value) throws SyntaxErrorException {
		if(value.equals("")) {
			throw new SyntaxErrorException("You must specify a database name");
		}
		json.put("db", value);
		return json;
	}
}
 