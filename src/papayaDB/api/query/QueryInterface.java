package papayaDB.api.query;

import java.util.function.Consumer;

/**
 * Interface représentant une interface de requête de papayaDB. La manière dont la requête est traitée par l'interface n'est pas définie. (c'est le principe d'une interface, de faire des promesses sans les
 * préciser).
 *
 */
public interface QueryInterface {
	/**
	 * Traite la requête demandée par l'appel de méthode. Le format de la requête et sa méthode d'envoi son laissés à l'appréciation des classes implémentant l'interface.
	 * @param query la requête
	 * @param callback le {@link Consumer} qui recevra le {@link QueryAnswer} de réponse de la requête.
	 */
	public void processQuery(String query, Consumer<QueryAnswer> callback);
	public default void close() {}
	
	public static QueryInterface newHttpQueryInterface(String host, int port) {
		return new HttpQueryInterface(host, port);
	}
	
	public static QueryInterface newTcpQueryInterface(String host, int port) {
		return new TcpQueryInterface(host, port);
	}
}