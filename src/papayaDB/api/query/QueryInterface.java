package papayaDB.api.query;

import java.util.function.Consumer;

import io.vertx.core.json.JsonObject;

/**
 * Interface représentant une interface de requête de papayaDB. La manière dont la requête est traitée par l'interface n'est pas définie. (c'est le principe d'une interface, de faire des promesses sans les
 * préciser).
 *
 */
public interface QueryInterface {
	
	public void createNewDatabase(String name, String user, String hash, Consumer<QueryAnswer> callback);
	public void deleteDatabase(String name, String user, String hash, Consumer<QueryAnswer> callback);
	public void exportDatabase(String database, String user, String hash, Consumer<QueryAnswer> callback);
	
	public void updateRecord(String database, String uid, JsonObject newRecord, String user, String hash, Consumer<QueryAnswer> callback);
	public void deleteRecords(String database, JsonObject parameters, String user, String hash, Consumer<QueryAnswer> callback);
	public void insertNewRecord(String database, JsonObject record, String user, String hash, Consumer<QueryAnswer> callback);
	public void getRecords(String database, JsonObject parameters, String user, String hash, Consumer<QueryAnswer> callback);
	
	public default void close() {}
	
	public static QueryInterface newHttpQueryInterface(String host, int port) {
		return new HttpQueryInterface(host, port);
	}
	
	public static QueryInterface newTcpQueryInterface(String host, int port) {
		return new TcpQueryInterface(host, port);
	}
}