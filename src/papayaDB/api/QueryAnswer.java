package papayaDB.api;

import io.vertx.core.json.JsonObject;

/**
 * Contien l'Objet Json de réponse et le code de status.
 * Le code de status es contenu aussi dans l'objet JSON, il est présent dans la classe pour des raisons de confort.
 */
public class QueryAnswer {
	/**
	 * Objet JSON renvoyé.
	 */
	private JsonObject data;
	/**
	 * Status de la réponse. Stocké en plus dans ce champs pour faciliter les tests sur la réponse de la requête.
	 */
	private QueryAnswerStatus status;
	
	/**
	 * Constructeur de la {@link QueryAnswer}
	 * @param answer La réponse qui sera contenue dans l'objet.
	 */
	public QueryAnswer(JsonObject answer) {
		if(!answer.containsKey("status") || (answer.getString("status") == QueryAnswerStatus.OK.name() && !answer.containsKey("data"))) {
			throw new IllegalArgumentException("JSON Object provided to build Query answer is malformed :"+answer.toString());
		}
		this.status = QueryAnswerStatus.valueOf(answer.getString("status"));
		this.data = answer;
	}
	
	@Override
	public String toString() {
		return status.name()+": "+data.encodePrettily();
	}
	
	public JsonObject getData() {
		return this.data;
	}
}