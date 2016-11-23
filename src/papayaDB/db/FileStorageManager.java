package papayaDB.db;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryParameters;

/**
 * Implémentation d'un StorageManager mappant le contenu de la base de données qu'il représente dans un fichier
 */
public class FileStorageManager implements StorageManager {
	private final static int CHUNK_SIZE = 64;
	private final RandomAccessFile rafile;
	private FileChannel file;
	private int recordsNumber;
	private MappedByteBuffer recordsNumberMapping;
	
	
	public FileStorageManager(String fileName) throws IOException {
		Path pathToFile = Paths.get(fileName+".coll");
		if (!Files.exists(pathToFile, LinkOption.NOFOLLOW_LINKS)) {
			Files.createFile(pathToFile);
			recordsNumber = -1;
		}
		
		rafile = new RandomAccessFile(pathToFile.toFile(), "rw");
		this.file = rafile.getChannel();
		
		// Read number of elements
		recordsNumberMapping = file.map(MapMode.READ_WRITE, 0, Integer.BYTES);
		if(recordsNumber == -1) {
			updateRecordsNumber(0);
		} else {
			recordsNumber = recordsNumberMapping.getInt();
		}
		file.position(0);
	}

	@Override
	public Map<UUID, Record> loadRecords() throws IOException {
		long fileSize = rafile.length();
		HashMap<UUID, Record> elements = new HashMap<UUID, Record>();
		
		if(fileSize == 0) {
			return elements;
		}
		
		MappedByteBuffer bufferedFile = file.map(MapMode.READ_ONLY, 0, fileSize);
		bufferedFile.getInt();
		try {
			System.out.println("Loading "+recordsNumber+" records...");
			for(int i = 0; i < recordsNumber; i++) {
				
				int objectSize = bufferedFile.getInt();
				
//				System.out.println("Loading object, real size "+objectSize+" bytes at pos "+(bufferedFile.position()-Integer.BYTES));
				
				int size = computeChunkSize(objectSize);
				MappedByteBuffer buffer = file.map(MapMode.READ_WRITE, bufferedFile.position()-Integer.BYTES, size);
				
				// Position = position + taille objet - lecture faite de la taille (int)
				bufferedFile.position(bufferedFile.position()+size-Integer.BYTES);
				
				Record record = new FileRecord(buffer, objectSize, this);
				elements.put(record.getUUID(), record);
			}
		} catch(BufferUnderflowException e) {
			System.out.println("Corrupted database collection??");
		}
		
		return elements;
	}


	@Override
	public Record createNewRecord(JsonObject newObject) {
		Record newRecord = null;
		Buffer b = Buffer.buffer();
		
		newObject.put("_uid", UUID.randomUUID().toString());
		b.appendBytes(newObject.toString().getBytes());
		int neededSize = computeChunkSize(b.length()+Integer.BYTES);
		System.out.println("New record will need "+(int) Math.ceil((double) b.length()/ (double) CHUNK_SIZE)+" chunks of size "+CHUNK_SIZE+" (real size: "+b.length()+")");
		try {
			long oldSize = file.size();
			rafile.setLength(oldSize+neededSize);
			file = rafile.getChannel();
			
			MappedByteBuffer buffer = file.map(MapMode.READ_WRITE, oldSize, neededSize);
			buffer.putInt(b.length());
			buffer.put(b.getBytes());
			newRecord = new FileRecord(buffer, b.length(), this);
			updateRecordsNumber(recordsNumber+1);
			
		} catch (IOException e) {
			System.out.println(e);
		}
		
		return newRecord;
	}
	
	private void updateRecordsNumber(int newNumber) {
		recordsNumber = newNumber;
		recordsNumberMapping.rewind();
		recordsNumberMapping.putInt(recordsNumber);
	}
	
	private int computeChunkSize(int recordSize) {
		return (int) Math.ceil((double) (recordSize)/ (double) CHUNK_SIZE)*CHUNK_SIZE;
	}

	@Override
	public void deleteRecord(Record record) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String toString() {
		try {
			return "FileStorageManager ("+file.toString()+"); allocated: "+file.size()+" bytes ("+recordsNumber+" records)";
		} catch (IOException e) {
			return "Error";
		}
	}
	
	@Override
	public int getRecordsNumber() {
		return recordsNumber;
	}


	/**
	 * Classe implémentant un Record par son stockage physique dans un FileChannel.
	 */
	class FileRecord implements Record {
		private boolean considerDeleted = false;
		private MappedByteBuffer buffer;
		private int realSize;
		private StorageManager storageManager;
		
		public FileRecord(MappedByteBuffer buffer, int realSize, FileStorageManager storageManager) {
			this.buffer = buffer;
			this.realSize = realSize;
			this.storageManager = storageManager;
		}

		/**
		 * Retourne une copie sous forme de JsonObject du buffer mappant la mémoire dessous.
		 */
		@Override
		public JsonObject getRecord() {
			// On retourne un objet JSON créé à partir d'un Buffer vertx, créé à partir de la section consommée du MappedByteBuffer (qui lui représente le total de chunks réservés pour l'objet)
			
			byte[] byteArr = ByteBuffer.allocate(realSize).array();
			buffer.rewind();
			// jump written size
			buffer.getInt();
			buffer.get(byteArr, 0, realSize);
			Buffer b = Buffer.buffer(byteArr);
			return b.toJsonObject();
		}
		
		@Override
		public void deleteRecord() {
			// TODO Auto-generated method stub
			
			considerDeleted = true;
		}

		@Override
		public void updateRecord(JsonObject newObject) {
			// TODO Auto-generated method stub

		}

		@Override
		public boolean considerDeleted() {
			return considerDeleted;
		}
		
		private int getChunkSize() {
			return buffer.capacity();
		}
		
		private int getRealSize() {
			return realSize;
		}

		@Override
		public StorageManager getStorageManager() {
			return storageManager;
		}
		
		@Override
		public String toString() {
			return getRecord().toString();
		}

		@Override
		public UUID getUUID() {
			return UUID.fromString(getRecord().getString("_uid"));
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		StorageManager sm = new FileStorageManager("testDb");
		System.out.println(sm);
		Record r = sm.createNewRecord(new JsonObject().put("Une clef", "Test"));
		Record r2 = sm.createNewRecord(new JsonObject().put("Une clef2", "Test"));
		System.out.println(sm);
		
		
		System.out.println(r.getRecord());
		System.out.println(r2.getRecord());
		
		System.out.println(sm.loadRecords());
	}
}