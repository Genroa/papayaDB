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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import papayaDB.api.QueryAnswerStatus;

/**
 * Implémentation d'un StorageManager mappant le contenu de la base de données qu'il représente dans un fichier
 */
public class FileStorageManager implements StorageManager {
	private final static int EMPTY_CHUNK_SECTION = -1;
	private final static int CHUNK_SIZE = 64;
	private final RandomAccessFile rafile;
	private FileChannel file;
	private int recordsNumber;
	private MappedByteBuffer recordsNumberMapping;
	private HashMap<Integer, ArrayList<Integer>> holesMap = new HashMap<>();
	
	
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
			for(int i = 0; i < recordsNumber;) {
				
				int objectSize = bufferedFile.getInt();
				if(objectSize == EMPTY_CHUNK_SECTION) {
					int nextObjectPositionInChunks = bufferedFile.getInt();
					System.out.println("Hole found : "+nextObjectPositionInChunks+" chunks");
					registerChunkSectionAsHole(bufferedFile.position()-(Integer.BYTES*2), nextObjectPositionInChunks);
					
					// Position + nouvelle position en chunks (relatifs) - la lecture de l'int indiquant la nouvelle position
					bufferedFile.position(bufferedFile.position()+nextObjectPositionInChunks*CHUNK_SIZE-(Integer.BYTES*2));
					continue;
				}
				
				
				int size = computeChunkSize(objectSize);
				// System.out.println("Loading object, chunk size "+size+" bytes at pos "+(bufferedFile.position()-Integer.BYTES));
				MappedByteBuffer buffer = file.map(MapMode.READ_WRITE, bufferedFile.position()-Integer.BYTES, size);
				
				// Position = position + taille objet - lecture faite de la taille (int)
				int objectPosition = bufferedFile.position()-Integer.BYTES+size;
				bufferedFile.position(objectPosition);
				
				Record record = new FileRecord(buffer, objectSize, objectPosition, this);
				elements.put(record.getUUID(), record);
				i++;
			}
		} catch(BufferUnderflowException e) {
			System.out.println("Corrupted database collection??");
		}
		
		return elements;
	}
	
	/**
	 * Permet d'enregistrer une section de chunks comme étant vide et réinscriptible
	 * @param positionInFile la position en bytes dans le fichier
	 * @param chunkNumber le nombre de chunks vides à partir de cette position
	 */
	private void registerChunkSectionAsHole(int positionInFile, int chunkNumber) {
		ArrayList<Integer> emptyChunksOfThisSize = holesMap.getOrDefault(chunkNumber, new ArrayList<>());
		emptyChunksOfThisSize.add(positionInFile);
		holesMap.put(chunkNumber, emptyChunksOfThisSize);
		System.out.println(holesMap);
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
			int position = findSuitablePositionToWrite(neededSize);
			MappedByteBuffer buffer = file.map(MapMode.READ_WRITE, position, neededSize);
			buffer.putInt(b.length());
			buffer.put(b.getBytes());
			newRecord = new FileRecord(buffer, b.length(), position, this);
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
		if(!(record instanceof FileRecord) || record.getStorageManager() != this) {
			throw new IllegalArgumentException("Record isn't compatible with this StorageManager or hasn't been produced by it");
		}
		
		FileRecord fileRecord = (FileRecord) record;
		int chunkSize = fileRecord.getChunkSize();
		int position = fileRecord.getPositionInFile();
		registerChunkSectionAsHole(position, chunkSize/CHUNK_SIZE);
		updateRecordsNumber(recordsNumber-1);
	}

	@Override
	public String toString() {
		try {
			return "FileStorageManager ("+file.toString()+"); allocated: "+file.size()+" bytes ("+recordsNumber+" records)";
		} catch (IOException e) {
			return "Error";
		}
	}
	
	public void lookAtHoles() {
		for(Entry<Integer, ArrayList<Integer>> entry : holesMap.entrySet()) {
			System.out.println(entry.getKey()+" => "+entry.getValue());
		}
	}
	
	@Override
	public int getRecordsNumber() {
		return recordsNumber;
	}
	
	/**
	 * Retourne la position où écrire le nouveau Record. S'occupe éventuellement de l'agrandissement/allocation mémoire si nécessaire.
	 * @param sizeOfChunkInBytes la taille nécessaire à trouver
	 * @return la position où écrire
	 * @throws IOException en cas d'erreur lors de la recherche
	 */
	private int findSuitablePositionToWrite(int sizeOfChunkInBytes) throws IOException {
		int chunkNumber = sizeOfChunkInBytes/CHUNK_SIZE;
		
		int insertionPosition = -1;
		
		List<Entry<Integer, ArrayList<Integer>>> entries = holesMap.entrySet().stream()
																			.filter(entry -> entry.getKey() >= chunkNumber)
																			.sorted((entry1, entry2) -> {
																				return 0;
																			})
																			.collect(Collectors.toList());
		
		for(Entry<Integer, ArrayList<Integer>> entry : entries) {
			ArrayList<Integer> holes = entry.getValue();
			int holeSize = entry.getKey();
			
			// Si pas de place de cette taille
			if(holes.size() == 0) {
				System.out.println("No hole of size "+holeSize+" available");
				continue;
			}
			
			System.out.println("Holes of size "+holeSize+" found, insertion...");
			// On a trouvé une place; modifier la holesMap en conséquence
			// Size = hole : on a juste comblé un trou
			if(holeSize == chunkNumber) {
				insertionPosition = holes.remove(0);
			} else {
				// Size > hole : on doit déplacer le trou dans le nouvel emplacement et placerle insertionPosition correctement
				insertionPosition = holes.remove(0);
				int newHolePosition = insertionPosition+chunkNumber*CHUNK_SIZE;
				int newHoleSize = holeSize-chunkNumber;
				registerChunkSectionAsHole(newHolePosition, newHoleSize);
				
				// Ecrire de nouveau les informations de chunk vide
				MappedByteBuffer buffer = file.map(MapMode.READ_WRITE, newHolePosition, Integer.BYTES*2);
				buffer.putInt(EMPTY_CHUNK_SECTION);
				buffer.putInt(newHoleSize);
			}
			break;
		}
		
		// On n'a pas trouvé de trou qui convienne : insertion en fin de fichier
		if(insertionPosition == -1) {
			System.out.println("Didn't find fitting hole : appending");
			insertionPosition = (int) file.size();
			rafile.setLength(insertionPosition+sizeOfChunkInBytes);
			file = rafile.getChannel();
		}
		
		return insertionPosition;
	}
	
	/**
	 * Classe implémentant un Record par son stockage physique dans un FileChannel.
	 */
	class FileRecord implements Record {
		private MappedByteBuffer buffer;
		private int realSize;
		private StorageManager storageManager;
		private int positionInFile;
		
		public FileRecord(MappedByteBuffer buffer, int realSize, int positionInFile, FileStorageManager storageManager) {
			this.buffer = buffer;
			this.realSize = realSize;
			this.storageManager = storageManager;
			this.positionInFile = positionInFile;
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
			// Vider pour debug
//			buffer.rewind();
//			for(int i =0; i<buffer.capacity(); i++) {
//				buffer.put((byte) 0);
//			}
			
			buffer.rewind();
			buffer.putInt(-1);
			buffer.putInt(getChunkSize()/CHUNK_SIZE);
			storageManager.deleteRecord(this);
		}

		@Override
		public void updateRecord(JsonObject newObject) {
			// TODO Auto-generated method stub
			
		}
		
		/**
		 * Donne la taille en bytes de la section de chunks allouée pour l'entrée
		 * @return la taille en byte de la section de chunks alloués
		 */
		private int getChunkSize() {
			return buffer.capacity();
		}
		
		private int getRealSize() {
			return realSize;
		}
		
		private int getPositionInFile() {
			return positionInFile;
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
		FileStorageManager sm = new FileStorageManager("testDb");
		sm.loadRecords();
		
//		Record r = sm.createNewRecord(new JsonObject().put("une clef", "test valeur"));
//		Record r2 = sm.createNewRecord(new JsonObject().put("une clef", "test valeur"));
//		Record r3 = sm.createNewRecord(new JsonObject().put("une clef", "test valeur"));
//		r2.deleteRecord();
		
		Record smallRecord = sm.createNewRecord(new JsonObject().put("a", "t"));
		
		
		
		sm.lookAtHoles();
	}
}