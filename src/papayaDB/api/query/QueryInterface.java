package papayaDB.api.query;

import java.util.function.Consumer;

import io.vertx.core.json.JsonObject;

/**
 * Interface représentant une interface de requête de papayaDB. La manière dont la requête est traitée par l'interface n'est pas définie. (c'est le principe d'une interface, de faire des promesses sans les
 * préciser).
 *
 */
public interface QueryInterface {
	
	public void setAuthInformations(String user, String hash);
	
	public void createNewDatabase(String name, Consumer<QueryAnswer> callback);
	public void deleteDatabase(String name, Consumer<QueryAnswer> callback);
	public void exportDatabase(String database, Consumer<QueryAnswer> callback);
	
	public void updateRecord(String database, String uid, JsonObject newRecord, Consumer<QueryAnswer> callback);
	public void deleteRecords(String database, JsonObject parameters, Consumer<QueryAnswer> callback);
	public void insertNewRecord(String database, JsonObject record, Consumer<QueryAnswer> callback);
	public void getRecords(String database, JsonObject parameters, Consumer<QueryAnswer> callback);
	
	public default void close() {}
	
	public static QueryInterface newHttpQueryInterface(String host, int port, String user, String hash) {
		return new HttpQueryInterface(host, port, user, hash);
	}
	
	public static QueryInterface newTcpQueryInterface(String host, int port, String user, String hash) {
		return new TcpQueryInterface(host, port, user, hash);
	}
}