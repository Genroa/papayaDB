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
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Implémentation d'un StorageManager mappant le contenu de la base de données qu'il représente dans un fichier
 */
public class FileStorageManager {
	private final static int EMPTY_CHUNK_SECTION = -1;
	private final static int CHUNK_SIZE = 64;
	private final RandomAccessFile rafile;
	private FileChannel file;
	private int recordsNumber;
	private MappedByteBuffer fileBuffer;
	private HashMap<Integer, ArrayList<Integer>> holesMap = new HashMap<>();
	private HashMap<Integer, Integer> addressMapping = new HashMap<>();
	private final String fileName;
	
	public FileStorageManager(String fileName) throws IOException {
		this.fileName = fileName;
		
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
		loadRecords();
	}
	
	private void loadRecords() throws IOException {
		long fileSize = rafile.length();
		
		if(fileSize == 0) {
			return;
		}
		
		fileBuffer.rewind();
		fileBuffer.getInt();
		try {
			for(int i = 0; i < recordsNumber;) {
				
				int objectSize = fileBuffer.getInt();
				if(objectSize == EMPTY_CHUNK_SECTION) {
					int nextObjectPositionInChunks = fileBuffer.getInt();
					registerChunkSectionAsHole(fileBuffer.position()-(Integer.BYTES*2), nextObjectPositionInChunks);
					
					fileBuffer.position(fileBuffer.position()+nextObjectPositionInChunks*CHUNK_SIZE-(Integer.BYTES*2));
					continue;
				}
				int objectPosition = fileBuffer.position()-Integer.BYTES;
				addressMapping.put(objectPosition, objectSize);
				
				fileBuffer.position(objectPosition+computeChunkSize(objectSize));
				i++;
			}
		} catch(BufferUnderflowException e) {
			System.out.println("[DB:StorageManager("+fileName+"):loadRecords]Corrupted database collection??");
		}
	}
	
	/**
	 * Permet d'enregistrer une section de chunks comme étant vide et réinscriptible. L'adressMapping est mis à jour durant l'appel. ATTENTION, ne modifie pas physiquement le fichier, c'est à faire à la main!
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
		addressMapping.remove(positionInFile);
		return new int[]{holePosition, chunkSize};
	}
	
	public JsonObject getRecordAtAddress(int address) {
		int realSize = addressMapping.getOrDefault(address, -1);
		if(realSize == -1) return null;
		
		byte[] byteArr = ByteBuffer.allocate(realSize).array();
		fileBuffer.position(address);
		
		// jump written size
		fileBuffer.getInt();
		fileBuffer.get(byteArr, 0, realSize);
		Buffer b = Buffer.buffer(byteArr);
		return b.toJsonObject();
	}
	
	public int createNewRecord(JsonObject newObject) {
		Buffer jsonObjectAsBuffer = buildBufferForNewRecord(newObject);
		int address = -1;
		
		address = insertRecord(jsonObjectAsBuffer);
		
		// Copie bien passée
		if(address != -1) {
			updateRecordsNumber(recordsNumber+1);
			addressMapping.put(address, jsonObjectAsBuffer.length());
		}
		return address;
	}
	
	private Buffer buildBufferForNewRecord(JsonObject newObject) {
		Buffer b = Buffer.buffer();
		newObject.put("_uid", UUID.randomUUID().toString());
		b.appendBytes(newObject.toString().getBytes());
		return b;
	}
	
	private int insertRecord(Buffer jsonObjectAsBuffer) {
		int realSize = jsonObjectAsBuffer.length();
		int neededSize = computeChunkSize(realSize+Integer.BYTES);
		try {
			int position = findSuitablePositionToWrite(neededSize);
			fileBuffer.position(position);
			fileBuffer.putInt(realSize);
			fileBuffer.put(jsonObjectAsBuffer.getBytes());
			return position;
		} catch(IOException e) {
			return -1;
		}
	}
	
	private void updateRecordsNumber(int newNumber) {
		recordsNumber = newNumber;
		int oldPos = fileBuffer.position();
		fileBuffer.rewind();
		fileBuffer.putInt(newNumber);
		fileBuffer.position(oldPos);
	}
	
	public static int computeChunkSize(int recordSize) {
		return (int) Math.ceil((double) (recordSize)/ (double) CHUNK_SIZE)*CHUNK_SIZE;
	}
	
	public void deleteRecordAtAddress(int address) {
		int realSize = addressMapping.getOrDefault(address, -1);
		if(realSize == -1) return;
		
		int chunkSize = computeChunkSize(realSize);
		
		fileBuffer.position(address);
		for(int i = 0; i < chunkSize; i++) {
			fileBuffer.put((byte)0);
		}
		
		int[] infos = registerChunkSectionAsHole(address, chunkSize/CHUNK_SIZE);
		
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
	
	public int getRecordsNumber() {
		return recordsNumber;
	}
	
	/**
	 * Retourne une copie de l'addressMapping. Ne verrouille pas son utilisation.
	 * @return
	 */
	public HashMap<Integer, Integer> getRecordsMap() {
		return new HashMap<>(addressMapping);
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
	
	public int updateRecord(int oldAddress, JsonObject newObject)
	{
		int oldSize = addressMapping.getOrDefault(oldAddress, -1);
		if(oldSize == -1) {
			return -1;
		}
		byte[] bytes = newObject.toString().getBytes();
		
		int oldSizeInChunks = computeChunkSize(oldSize)/CHUNK_SIZE;
		int newSizeInChunks = computeChunkSize(bytes.length+Integer.BYTES)/CHUNK_SIZE;
		
		// Si la taille en chunks est la même il suffit de réécrire par-dessus
		System.out.println("OLD SC "+oldSizeInChunks+" NEW SC "+newSizeInChunks);
		if(oldSizeInChunks == newSizeInChunks) {
			fileBuffer.position(oldAddress);
			fileBuffer.putInt(bytes.length);
			fileBuffer.put(bytes);
			addressMapping.put(oldAddress, bytes.length);
			return oldAddress;
		}
		else if(oldSizeInChunks > newSizeInChunks) {
			int chunkSizeDiff = oldSizeInChunks - newSizeInChunks;
			fileBuffer.position(oldAddress);
			fileBuffer.putInt(bytes.length);
			fileBuffer.put(bytes);
			addressMapping.put(oldAddress, bytes.length);
			
			//Puis inscrire le nouveau trou
			int holePos = oldAddress + chunkSizeDiff*CHUNK_SIZE;
			fileBuffer.position(holePos);
			fileBuffer.putInt(EMPTY_CHUNK_SECTION);
			fileBuffer.putInt(chunkSizeDiff);
			registerChunkSectionAsHole(holePos, chunkSizeDiff);
			return oldAddress;
		}
		// oldSizeInChunks < newSizeInChunks : pas la place
		else {
			fileBuffer.position(oldAddress);
			byte[] oldRecordInBytes = new byte[bytes.length];
			fileBuffer.get(oldRecordInBytes, 0, bytes.length);
			int newPos;
			try {
				lookAtHoles();
				newPos = findSuitablePositionToWrite(newSizeInChunks*CHUNK_SIZE);
				lookAtHoles();
			} catch (IOException e) {
				// Pas réussi à modifier (IOException sur findSuitableSize) : on réinscrit l'ancien au même endroit
				fileBuffer.position(oldAddress);
				fileBuffer.putInt(oldRecordInBytes.length);
				fileBuffer.put(oldRecordInBytes);
				return oldAddress;
			}
			System.out.println("New position found for update is "+newPos);
			// Ecriture au nouvel endroit
			fileBuffer.position(newPos);
			fileBuffer.putInt(bytes.length);
			fileBuffer.put(bytes);
			
			// Suppression de l'ancien exemplaire
			fileBuffer.position(oldAddress);
			fileBuffer.putInt(EMPTY_CHUNK_SECTION);
			fileBuffer.putInt(oldSizeInChunks);
			registerChunkSectionAsHole(oldAddress, oldSizeInChunks);
			addressMapping.put(newPos, bytes.length);
			return newPos;
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		FileStorageManager sm = new FileStorageManager("testDb");
		System.out.println("BEG");
		System.out.println(sm.holesMap);
		System.out.println(sm.addressMapping);
		
		for(Integer address : sm.addressMapping.keySet()) {
			System.out.println(sm.getRecordAtAddress(address));
		}
		
		int r = sm.createNewRecord(new JsonObject().put("a", "t"));
		System.out.println("INSERT 1");
		System.out.println(sm.holesMap);
		System.out.println(sm.addressMapping);
		
		int r2 = sm.createNewRecord(new JsonObject().put("a", "t"));
		System.out.println("INSERT 2");
		System.out.println(sm.holesMap);
		System.out.println(sm.addressMapping);
		
		int r3 = sm.createNewRecord(new JsonObject().put("a", "t"));
		System.out.println("INSERT 3");
		System.out.println(sm.holesMap);
		System.out.println(sm.addressMapping);
		
		int r4 = sm.createNewRecord(new JsonObject().put("a", "t"));
		System.out.println("INSERT 4");
		System.out.println(sm.holesMap);
		System.out.println(sm.addressMapping);
		
		int r5 = sm.createNewRecord(new JsonObject().put("a", "t"));
		System.out.println("INSERT 5");
		System.out.println(sm.holesMap);
		System.out.println(sm.addressMapping);

		sm.deleteRecordAtAddress(r4);
		System.out.println("DELETE 4");
		System.out.println(sm.holesMap);
		System.out.println(sm.addressMapping);
//		
//		r2.deleteRecord();
//		System.out.println("DELETE 2");
//		System.out.println(sm.holesMap);
//		System.out.println(sm.addressMapping);
//		
//		r3.deleteRecord();
//		System.out.println("DELETE 3");
//		System.out.println(sm.holesMap);
//		System.out.println(sm.addressMapping);
//		
//		Record r7 = sm.createNewRecord(new JsonObject().put("a", "t"));
//		System.out.println("INSERT 7");
//		System.out.println(sm.holesMap);
//		System.out.println(sm.addressMapping);
//		
//		r5.updateRecord(r5.getRecord().put("newKey", "newValue"));
//		System.out.println("UPDATE 5");
//		System.out.println(sm.holesMap);
//		System.out.println(sm.addressMapping);
	}
}