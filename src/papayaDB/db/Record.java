package papayaDB.db;

import java.util.UUID;

import io.vertx.core.json.JsonObject;

/**
 * Interface représentant une entrée de base de données. L'implémentation derrière est laissée à la charge du StorageManager, qui fournit l'implémentation en accord avec sa gestion du stockage derrière.
 * En laissant le StorageManager fournir son implémentation, on évite de la duplication d'allocation, en travaillant directement à partir de la vision de la mémoire du StorageManager si possible.
 */
public interface Record {
	public JsonObject getRecord();
	public void deleteRecord();
	public void updateRecord(JsonObject newObject);
	public StorageManager getStorageManager();
	public UUID getUUID();
}
