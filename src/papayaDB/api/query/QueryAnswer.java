package papayaDB.api.query;

import java.util.List;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Contien l'Objet Json de réponse et le code de status.
 * Le code de status es contenu aussi dans l'objet JSON, il est présent dans la classe pour des raisons de confort.
 */
/**
 * 
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
	
	/** Méthode permettant de récuperer les données.
	 * @return
	 * JsonObject contenant les données
	 */
	public JsonObject getData() {
		return this.data;
	}
	
	/** Méthode statique permettant de construire une réponse d'erreur
	 * @param status
	 * 			Code de la réponse
	 * @param message
	 *			Message d'erreur
	 * @return
	 * 			Renvoie une QueryAnswer contenant le Json de l'erreur générée
	 */
	public static QueryAnswer buildNewErrorAnswer(QueryAnswerStatus status, String message) {
		if(status == QueryAnswerStatus.OK) throw new IllegalArgumentException("OK status can't be used as an error status");
		return new QueryAnswer(new JsonObject().put("status", status.name()).put("message", message));
	}
	
	/** Méthode de construction d'une réponse valide
	 * @param objects
	 * 			Liste des objets à ajouter à la réponse
	 * @return
	 * 			Un QueryAnswer contenant le JsonObject de la réponse
	 */
	public static QueryAnswer buildNewDataAnswer(List<JsonObject> objects) {
		return new QueryAnswer(new JsonObject().put("status", QueryAnswerStatus.OK.name()).put("data", new JsonArray(objects)));
	}
	
	/** Méthode de construction d'une réponse vide
	 * @return
	 * 		Un QueryAnswer contenant un json dont le champ data est vide
	 */
	public static QueryAnswer buildNewEmptyOkAnswer() {
		return new QueryAnswer(new JsonObject().put("status", QueryAnswerStatus.OK.name()).put("data", new JsonArray()));
	}
}