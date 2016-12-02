package papayaDB.db;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import io.vertx.core.json.JsonObject;

/**
 * Interface représentant un objet se chargeant du stockage en mémoire d'un Record d'une base de données
 */
public interface StorageManager {
	public Map<UUID, Record> loadRecords() throws IOException;
	public Record createNewRecord(JsonObject newRecord);
	public void deleteRecord(Record record);
	public void updateRecord(Record record, JsonObject newObject);
	public int getRecordsNumber();
}