package papayaDB.db;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Implémentation d'un StorageManager mappant le contenu de la base de données qu'il représente dans un fichier
 */
public class FileStorageManager implements StorageManager {
	
	private final FileChannel file;
	
	public FileStorageManager(String fileName) throws IOException {
		this.file = FileChannel.open(Paths.get(fileName+".coll"), StandardOpenOption.CREATE);
	}
}


/**
 * Classe représentant une entrée de base de données. Représenté par son stockage éventuel ou
 */
class FileRecord implements Record {

}