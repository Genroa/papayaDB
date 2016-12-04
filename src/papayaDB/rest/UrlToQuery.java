package papayaDB.rest;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.vertx.core.json.JsonObject;
import papayaDB.api.query.QueryAnswerStatus;
import papayaDB.api.query.QueryType;
import papayaDB.api.query.SyntaxErrorException;
import papayaDB.api.queryParameters.QueryParameter;

/**
 * Cette classe permet de transformer une Url en document JSON définissant une requete compréhensible par le serveur de base de donnée
 */
public class UrlToQuery {	
	/**
	 * Expression régulière permettant de découper les slash en ne prenant pas en compte les slashs entre quotes (%22)
	 */
	private static final String REGEX = "/";
	
	
	/**
	 * Méthode de conversion d'une URL en JSON en fonction du type de requete.
	 * @param url
	 * 			URL de la requete
	 * @param type
	 * 			Type de la requete (voir {@link QueryType})
	 * @return
	 * 			Renvoie un {@link Optional} de {@link JsonObject} représentant la requete à envoyer au serveur.
	 */
	public static JsonObject convertToJson(String url, QueryType type) {
		JsonObject json = new JsonObject();
		Objects.requireNonNull(url);
		System.out.println("[URLToQuery:ReceivedURL]"+url);
		String[] params = url.substring(1).split(REGEX); //remove the first '/' and split at all others except if between " ".
		if(params.length % 2 != 0) {
			System.out.println(Arrays.toString(params));
			generateErrorMessage(json, "invalid number of query parameters : " + params.length);
			return json;
		} 
		if(!put(json, "db", params[1], type)) {
			return json;
		}
		if(!put(json, "type", type.toString(), type)) {
			return json;
		}
		for (int i = 2; i < params.length; i += 2) {
			if(!put(json, params[i], params[i+1], type)) {
				break;
			}
		}
		return json;
	}

	/**
	 * Méthode privée permettant d'ajouter un élément dans le fichier JSON
	 * @param json
	 * 			Document JSON à mettre à jour
	 * @param key
	 * 			Clé d'ajout
	 * @param value
	 * 			Valeur correspondant à j'ajout (correspond au morceau de la requete)
	 * @param type
	 * 			Type de requete
	 */
	private static boolean put(JsonObject json, String key, String value, QueryType type) {
		Optional<QueryParameter> query = QueryParameter.getQueryParameterKey(type, key);
		if(!query.isPresent()) {
			generateErrorMessage(json, "parameter " + key + " doesn't exists");
			return false;
		}
		try {
			query.get().valueToJson(json, value);
		} catch (SyntaxErrorException e) {
			generateErrorMessage(json, e.getMessage());
			return false;
		}
		return true;
	}
	
	private static void generateErrorMessage(JsonObject json, String message) {
		json.clear();
		json.put("status", QueryAnswerStatus.SYNTAX_ERROR);
		json.put("message", message);
	}
}
