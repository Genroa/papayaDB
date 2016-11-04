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
	 * Status de la réponse.
	 */
	private QueryAnswerStatus status;
	
	/**
	 * Constructeur de la {@link QueryAnswer}
	 * @param answer La réponse qui sera contenue dans l'objet.
	 */
	public QueryAnswer(JsonObject answer) {
		this.data = answer;
		this.status = QueryAnswerStatus.OK;
		//TODO définir le vrai status en le lisant dans answer
	}
	
	@Override
	public String toString() {
		return status.name()+": "+data.encodePrettily();
	}
	
	public JsonObject getData() {
		return this.data;
	}
}