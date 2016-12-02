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

/**
 * Implémentation d'un StorageManager mappant le contenu de la base de données qu'il représente dans un fichier
 */
public class FileStorageManager implements StorageManager {
	private final static int EMPTY_CHUNK_SECTION = -1;
	private final static int CHUNK_SIZE = 64;
	private final RandomAccessFile rafile;
	private FileChannel file;
	private int recordsNumber;
	private MappedByteBuffer fileBuffer;
	private HashMap<Integer, ArrayList<Integer>> holesMap = new HashMap<>();
	
	
	public FileStorageManager(String fileName) throws IOException {
		Path pathToFile = Paths.get(fileName+".coll");
		if (!Files.exists(pathToFile, LinkOption.NOFOLLOW_LINKS)) {
			Files.createFile(pathToFile);
			recordsNumber = -1;
		}
		
		rafile = new RandomAccessFile(pathToFile.toFile(), "rw");
		this.file = rafile.getChannel();
		
		if(rafile.length() == 0) {
			rafile.setLength(1);
			file.map(MapMode.READ_WRITE, 0, Integer.BYTES).putInt(0);
		}
		
		// Read number of elements
		fileBuffer = file.map(MapMode.READ_WRITE, 0, file.size());
		
		if(recordsNumber == -1) {
			updateRecordsNumber(0);
		} else {
			fileBuffer.rewind();
			recordsNumber = fileBuffer.getInt();
		}
		fileBuffer.rewind();
	}

	@Override
	public Map<UUID, Record> loadRecords() throws IOException {
		long fileSize = rafile.length();
		HashMap<UUID, Record> elements = new HashMap<UUID, Record>();
		
		if(fileSize == 0) {
			return elements;
		}
		
		fileBuffer.rewind();
		fileBuffer.getInt();
		try {
//			System.out.println("Loading "+recordsNumber+" records...");
			for(int i = 0; i < recordsNumber;) {
				
				int objectSize = fileBuffer.getInt();
				if(objectSize == EMPTY_CHUNK_SECTION) {
					int nextObjectPositionInChunks = fileBuffer.getInt();
//					System.out.println("Hole found : "+nextObjectPositionInChunks+" chunks");
					registerChunkSectionAsHole(fileBuffer.position()-(Integer.BYTES*2), nextObjectPositionInChunks);
					
					// Position + nouvelle position en chunks (relatifs) - la lecture de l'int indiquant la nouvelle position
					fileBuffer.position(fileBuffer.position()+nextObjectPositionInChunks*CHUNK_SIZE-(Integer.BYTES*2));
					continue;
				}
				int objectPosition = fileBuffer.position()-Integer.BYTES;
				System.out.println("Loading object, chunk size "+computeChunkSize(objectSize)+" bytes at pos "+objectPosition);
				Record record = new FileRecord(objectSize, objectPosition, this);
				elements.put(record.getUUID(), record);
				fileBuffer.position(objectPosition+computeChunkSize(objectSize));
				i++;
			}
		} catch(BufferUnderflowException e) {
			System.out.println("Corrupted database collection??");
		}
		
		return elements;
	}
	
	/**
	 * Permet d'enregistrer une section de chunks comme étant vide et réinscriptible. ATTENTION, ne modifie pas physiquement le fichier, c'est à faire à la main!
	 * @param positionInFile la position en bytes dans le fichier
	 * @param chunkSize le nombre de chunks vides à partir de cette position
	 */
	private int[] registerChunkSectionAsHole(Integer positionInFile, Integer chunkSize) {
		int holePosition = positionInFile;
		
		// Essaie de trouver si la section suivante est le début d'un trou ou pas
		int nextSectionPosition = positionInFile+chunkSize*CHUNK_SIZE;
		Entry<Integer, ArrayList<Integer>> nextHoleEntry = holesMap.entrySet().stream()
																		.filter(currentEntry -> currentEntry.getValue().contains(nextSectionPosition))
																		.findFirst()
																		.orElse(null);
		
		// Si la fin de la section à effacer est bien le début d'un trou, on agrandit le trou
		if(nextHoleEntry != null) {
			int holeSizeInChunks = nextHoleEntry.getKey();
			ArrayList<Integer> holes = nextHoleEntry.getValue();
//			System.out.println("Found a hole section just right after the one to create : "+nextSectionPosition+" in "+holes+" (chunks size "+holeSizeInChunks+")");
			holes.remove((Integer) nextSectionPosition);
			chunkSize += holeSizeInChunks;
		}
		
		// Essaie de trouver la section précédente vide si elle existe
		Entry<Integer, ArrayList<Integer>> previousHoleEntry = holesMap.entrySet().stream()
																				.filter(currentEntry -> currentEntry.getValue().contains(positionInFile - currentEntry.getKey()*CHUNK_SIZE))
																				.findFirst()
																				.orElse(null);
		// S'il y a bien une entrée avant
		if(previousHoleEntry != null) {
			int holeSizeInChunks = previousHoleEntry.getKey();
			ArrayList<Integer> holes = previousHoleEntry.getValue();
			int previousSectionPosition = positionInFile - holeSizeInChunks*CHUNK_SIZE;
			
			holes.remove((Integer) previousSectionPosition);
			holePosition = previousSectionPosition;
			chunkSize += holeSizeInChunks;
		}
		
		
		
		ArrayList<Integer> emptyChunksOfThisSize = holesMap.getOrDefault(chunkSize, new ArrayList<>());
		emptyChunksOfThisSize.add(holePosition);
		holesMap.put(chunkSize, emptyChunksOfThisSize);
		return new int[]{holePosition, chunkSize};
	}

	@Override
	public Record createNewRecord(JsonObject newObject) {
		FileRecord newRecord = null;
		Buffer jsonObjectAsBuffer = buildBufferForNewRecord(newObject);
		
		newRecord = new FileRecord(jsonObjectAsBuffer.length(), 0, this); // fake positionInFile
		newRecord = insertRecord(newRecord, jsonObjectAsBuffer);
		
		// Copie bien passée
		if(newRecord != null) {
			updateRecordsNumber(recordsNumber+1);
		}
		return newRecord;
	}
	
	private Buffer buildBufferForNewRecord(JsonObject newObject) {
		Buffer b = Buffer.buffer();
		newObject.put("_uid", UUID.randomUUID().toString());
		b.appendBytes(newObject.toString().getBytes());
		return b;
	}
	
	private FileRecord insertRecord(FileRecord newRecord, Buffer jsonObjectAsBuffer) {
		int realSize = newRecord.realSize;
		int neededSize = computeChunkSize(realSize+Integer.BYTES);
		try {
			int position = findSuitablePositionToWrite(neededSize);
			fileBuffer.position(position);
			fileBuffer.putInt(realSize);
			
			// Si un jsonObject a été fourni : nouvelle écriture
			if(newRecord.positionInFile == 0 && jsonObjectAsBuffer != null) {
				fileBuffer.put(jsonObjectAsBuffer.getBytes());
			}
			// Sinon, on utilise l'ancienne info pour copier coller
			else {
				int oldPos = newRecord.positionInFile;
				byte[] buf = new byte[newRecord.realSize];
				fileBuffer.position(oldPos);
				fileBuffer.get(buf);
				fileBuffer.position(position);
				fileBuffer.getInt(); // skip size info
				fileBuffer.put(buf);
			}
			newRecord.positionInFile = position;
		} catch(IOException e) {
			return null;
		}
		return newRecord;
	}
	
	private void updateRecordsNumber(int newNumber) {
		recordsNumber = newNumber;
		int oldPos = fileBuffer.position();
		fileBuffer.rewind();
		fileBuffer.putInt(newNumber);
		fileBuffer.position(oldPos);
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
		int[] infos = registerChunkSectionAsHole(position, chunkSize/CHUNK_SIZE);
		
		MappedByteBuffer buffer;
		try {
			buffer = file.map(MapMode.READ_WRITE, infos[0], Integer.BYTES*2);
			buffer.putInt(EMPTY_CHUNK_SECTION);
			buffer.putInt(infos[1]);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
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
	 * Retourne la position où écrire le nouveau Record. S'occupe éventuellement de l'agrandissement/allocation mémoire si nécessaire. La section retournée est assurée comme allouée mais vide.
	 * @param sizeOfChunkInBytes la taille nécessaire à trouver
	 * @return la position où écrire
	 * @throws IOException en cas d'erreur lors de la recherche
	 */
	private int findSuitablePositionToWrite(int sizeOfChunkInBytes) throws IOException {
		int chunkSize = sizeOfChunkInBytes/CHUNK_SIZE;
		
		int insertionPosition = -1;
		
		List<Entry<Integer, ArrayList<Integer>>> entries = holesMap.entrySet().stream()
																			.filter(entry -> entry.getKey() >= chunkSize)
																			.map(entry -> {
																				entry.getValue().sort((val1, val2) -> {return val1-val2;});
																				return entry;
																			})
																			.collect(Collectors.toList());
		
		for(Entry<Integer, ArrayList<Integer>> entry : entries) {
			ArrayList<Integer> holes = entry.getValue();
			int holeSize = entry.getKey();
			
			// Si pas de place de cette taille
			if(holes.size() == 0) {
//				System.out.println("No hole of size "+holeSize+" available");
				continue;
			}
			
//			lookAtHoles();
//			System.out.println("Holes of size "+holeSize+" found, insertion...");
			// On a trouvé une place; modifier la holesMap en conséquence
			// Size = hole : on a juste comblé un trou
			if(holeSize == chunkSize) {
				System.out.println("Perfect hole!");
				insertionPosition = holes.remove(0);
			} else {
				// Size > hole : on doit déplacer le trou dans le nouvel emplacement et placer le insertionPosition correctement
				insertionPosition = holes.remove(0);
				int newHolePosition = insertionPosition+chunkSize*CHUNK_SIZE;
				int newHoleSize = holeSize-chunkSize;
				int[] infos = registerChunkSectionAsHole(newHolePosition, newHoleSize);
				newHolePosition = infos[0];
				newHoleSize = infos[1];
				// Ecrire de nouveau les informations de chunk vide
				MappedByteBuffer buffer = file.map(MapMode.READ_WRITE, newHolePosition, Integer.BYTES*2);
				buffer.putInt(EMPTY_CHUNK_SECTION);
				buffer.putInt(newHoleSize);
			}
			break;
		}
		
		// On n'a pas trouvé de trou qui convienne : insertion en fin de fichier
		if(insertionPosition == -1) {
//			System.out.println("Didn't find fitting hole : appending");
			insertionPosition = (int) file.size();
			rafile.setLength(insertionPosition+sizeOfChunkInBytes);
			fileBuffer = file.map(MapMode.READ_WRITE, 0, file.size());
		}
		
		return insertionPosition;
	}
	
	public void updateRecord(Record record, JsonObject newObject)
	{
		if(!(record instanceof FileRecord) || record.getStorageManager() != this) {
			throw new IllegalArgumentException("Record isn't compatible with this StorageManager or hasn't been produced by it");
		}
		
		FileRecord fileRecord = (FileRecord) record;
		byte[] bytes = newObject.toString().getBytes();
		
		int oldSizeInChunks = fileRecord.getChunkSize()/CHUNK_SIZE;
		int newSizeInChunks = computeChunkSize(bytes.length+Integer.BYTES)/CHUNK_SIZE;
		
		// Si la taille en chunks est la même il suffit de réécrire par-dessus
		System.out.println("OLD SC "+oldSizeInChunks+" NEW SC "+newSizeInChunks);
		if(oldSizeInChunks == newSizeInChunks) {
			fileBuffer.position(fileRecord.positionInFile);
			fileBuffer.putInt(bytes.length);
			fileBuffer.put(bytes);
			fileRecord.realSize = bytes.length;
		}
		else if(oldSizeInChunks > newSizeInChunks) {
			int chunkSizeDiff = oldSizeInChunks - newSizeInChunks;
			fileBuffer.position(fileRecord.positionInFile);
			fileBuffer.putInt(bytes.length);
			fileBuffer.put(bytes);
			fileRecord.realSize = bytes.length;
			
			//Puis inscrire le nouveau trou
			int holePos = fileRecord.positionInFile + chunkSizeDiff*CHUNK_SIZE;
			fileBuffer.position(holePos);
			fileBuffer.putInt(EMPTY_CHUNK_SECTION);
			fileBuffer.putInt(chunkSizeDiff);
			registerChunkSectionAsHole(holePos, chunkSizeDiff);
		}
		// oldSizeInChunks < newSizeInChunks : pas la place
		else {
			fileBuffer.position(fileRecord.positionInFile);
			byte[] oldRecordInBytes = new byte[bytes.length];
			int oldPos = fileRecord.positionInFile;
			fileBuffer.get(oldRecordInBytes, 0, bytes.length);
			int newPos;
			try {
				lookAtHoles();
				newPos = findSuitablePositionToWrite(newSizeInChunks*CHUNK_SIZE);
				lookAtHoles();
			} catch (IOException e) {
				// Pas réussi à modifier (IOException sur finSuitableSize) : on réinscrit l'ancien au même endroit
				fileBuffer.position(oldPos);
				fileBuffer.putInt(oldRecordInBytes.length);
				fileBuffer.put(oldRecordInBytes);
				return;
			}
			System.out.println("New position found for update is "+newPos);
			// Ecriture au nouvel endroit
			fileBuffer.position(newPos);
			fileBuffer.putInt(bytes.length);
			fileBuffer.put(bytes);
			fileRecord.positionInFile = newPos;
			fileRecord.realSize = bytes.length;
			
			// Suppression de l'ancien exemplaire
			fileBuffer.position(oldPos);
			fileBuffer.putInt(EMPTY_CHUNK_SECTION);
			fileBuffer.putInt(oldSizeInChunks);
			registerChunkSectionAsHole(oldPos, oldSizeInChunks);
		}
	}
	
	
	/**
	 * Classe implémentant un Record par son stockage physique dans un FileChannel.
	 */
	class FileRecord implements Record {
		private int realSize;
		private FileStorageManager storageManager;
		private int positionInFile;
		
		public FileRecord(int realSize, int positionInFile, FileStorageManager storageManager) {
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
			MappedByteBuffer fileBuffer = storageManager.fileBuffer;
			fileBuffer.position(positionInFile);
			
			// jump written size
			fileBuffer.getInt();
			fileBuffer.get(byteArr, 0, realSize);
			Buffer b = Buffer.buffer(byteArr);
			return b.toJsonObject();
		}
		
		@Override
		public void deleteRecord() {
			storageManager.deleteRecord(this);
		}

		@Override
		public void updateRecord(JsonObject newObject) {
			storageManager.updateRecord(this, newObject);
		}
		
		/**
		 * Donne la taille en bytes de la section de chunks allouée pour l'entrée
		 * @return la taille en byte de la section de chunks alloués
		 */
		private int getChunkSize() {
			return computeChunkSize(realSize);
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
		
		Record r = sm.createNewRecord(new JsonObject().put("a", "t"));
		Record r2 = sm.createNewRecord(new JsonObject().put("a", "t"));
		Record r3 = sm.createNewRecord(new JsonObject().put("a", "t"));
		Record r4 = sm.createNewRecord(new JsonObject().put("a", "t"));
		Record r5 = sm.createNewRecord(new JsonObject().put("a", "t"));
		Record r6 = sm.createNewRecord(new JsonObject().put("a", "t"));
		r4.deleteRecord();
		r2.deleteRecord();
		r3.deleteRecord();
		Record r7 = sm.createNewRecord(new JsonObject().put("a", "t"));
		r5.updateRecord(r5.getRecord().put("newKey", "newValue"));
//		Record r8 = sm.createNewRecord(new JsonObject().put("a", "t"));
		
		System.out.println(r5);
	}
}