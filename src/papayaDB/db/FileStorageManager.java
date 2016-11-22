package papayaDB.db;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Implémentation d'un StorageManager mappant le contenu de la base de données qu'il représente dans un fichier
 */
public class FileStorageManager implements StorageManager {
	
	private final FileChannel file;
	
	public FileStorageManager(String fileName) throws IOException {
		this.file = FileChannel.open(Paths.get(fileName+".coll"), StandardOpenOption.CREATE);
	}
	
	
	
	
	/**
	 * Classe implémentant un Record par son stockage physique dans un FileChannel.
	 */
	class FileRecord implements Record {
		private MappedByteBuffer buffer;
		
		public FileRecord(MappedByteBuffer buffer) {
			this.buffer = buffer;
		}
		
		/**
		 * Retourne une copie sous forme de JsonObject du buffer mappant la mémoire dessous.
		 */
		@Override
		public JsonObject getRecord() {
			if(!buffer.isLoaded()) {
				buffer.load();
			}
			return Buffer.buffer(buffer.array()).toJsonObject();
		}

		@Override
		public void deleteRecord() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void updateRecord(JsonObject newObject) {
			// TODO Auto-generated method stub
			
		}

	}
}