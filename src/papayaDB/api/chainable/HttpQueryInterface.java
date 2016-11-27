package papayaDB.api.chainable;


import java.util.Objects;
import java.util.function.Consumer;

import io.vertx.core.http.HttpClient;
import papayaDB.api.QueryAnswer;

/**
 * Cette classe représente une connexion utilisateur (un "noeud de tête") pour faire des requêtes sur un noeud papayaDB.
 */
public class HttpQueryInterface extends AbstractChainableQueryInterface {
	/**
	 * L'objet employé pour le traitement des requêtes HTTP.
	 */
	private final HttpClient client;
	/**
	 * Le port de connexion à l'hôte.
	 */
	private final int port;
	/**
	 * Le nom de l'hôte de la connexion.
	 */
	private final String host;
	
	/**
	 * Crée une nouvelle connexion vers une interface de requête papayaDB.
	 * @param host le nom de l'hôte REST pour la connexion
	 * @param port le port pour la connexion
	 */
	public HttpQueryInterface(String host, int port) {
		client = getVertx().createHttpClient();
		this.host = host;
		this.port = port;
	}
	
	/**
	 * Constructeur simplifié d'une nouvelle connexion, le port par défaut est 80.
	 * @param host le nom de l'hôte REST pour la connexion
	 */
	public HttpQueryInterface(String host) {
		this(host, 80);
	}
	
	@Override
	public void close() {
		client.close();
		super.close();
	}
	
	@Override
	public void processQuery(String query, Consumer<QueryAnswer> callback) {
		Objects.requireNonNull(callback);
		client.getNow(port, host, "/"+query, response -> {
			//TODO check response.statusCode() before calling bodyHandler
			response.bodyHandler(bodyBuffer -> { callback.accept(new QueryAnswer(bodyBuffer.toJsonObject())); });
		});
	}
}