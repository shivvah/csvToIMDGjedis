package com.globalpay;

import java.io.File;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.RandomStringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.globalpay.Token2;
import com.opencsv.CSVWriter;

import picocli.CommandLine;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

@CommandLine.Command(description = "Imports csv files into IMDG Maps, assuming first field is index", name = "csvToIMDG", mixinStandardHelpOptions = true, version = "checksum 3.0")
public class csvToIMDG implements Callable<Void> {

	boolean printsysout = false;
	public static final String PAN = "pan";
	public static final String SEQKEY = "seqkey";
	public static final String MT = "mt";
	public static final String GT = "gt";
	public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	@CommandLine.Parameters(index = "0", description = "The F1 File path")
	private File f1File;

	@CommandLine.Option(names = "1", description = "The F2 File path")
	private File f2File;

	@CommandLine.Option(names = "2", description = "The output File path")
	private File outputFile;

	@CommandLine.Option(names = { "-v", "--verbose" }, description = "Be verbose.")
	private boolean verbose = false;

	@CommandLine.Option(names = { "-mf1", "--F1MapName" }, description = "F1 Map Name of destination defaults to F1Map")
	private String f1MapName = "F1Map";

	@CommandLine.Option(names = { "-mf2", "--F2MapName" }, description = "F2 Map Name of destination defaults to F2Map")
	private String f2MapName = "F2Map";

	@CommandLine.Option(names = { "-mainmap",
			"--MainMapName" }, description = "main Map Name of destination defaults to PANGT")
	private String mainMapName = "PANGT";

	/*@CommandLine.Option(names = { "-addrandommapentries",
			"--AddMapEntries" }, description = "if true it indicates that randomly generated values to be added to the map")*/
	private boolean addMapEntries = true;

	@CommandLine.Option(names = { "-randommapentriescount",
			"--RandomMapEntriesCount" }, description = "if addrandommapentries is true then specifies number of entries to add")
	private long mapEntriesCount = 10;

	@CommandLine.Option(names = { "-printrandommapentries",
			"--PrintRandomMapEntries" }, description = "print addrandommapentries ")
	private boolean printRandomMapEntries = false;

	@CommandLine.Option(names = { "-threads", "--Threads" }, description = "number of threads to use ")
	private short threadNumber = 1;

	@CommandLine.Option(names = { "-syncMissingKeys",
			"--SyncMissingKeys" }, description = "retrieves entries from f1map and add them to mainmap if they are missing")
	private boolean syncMissingKeys = false;

	@CommandLine.Option(names = { "-printsynccounter",
			"--PrintSyncCounter" }, description = "print addrandommapentries ")
	private boolean printSyncCounter = false;

	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	//private static Jedis jedis;
	private static ShardedJedis jedis;
	Map<String, String> f1Map = new HashMap<String, String>();
	Map<String, Token2> f2Map = new HashMap<String, Token2>();
	Map<String, String> mainMap = new HashMap<String, String>();

	public static void main(String[] args) {
		CommandLine.call(new csvToIMDG(), args);
		System.out.println("Callable function executed");
	}

	private Void addMapEntriesThreads(Map<String, String> f1Map, Map<String, Token2> f2Map,
			Map<String, String> mainMap) {
		long icounter = 0;
		int j = 0;
		long startTimeInMs = 0;

		try {
			System.out.println("Entered Add Map Entries Threads");
			ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);

			for (int i = 0; i < mapEntriesCount; i++) {
				if (mapEntriesCount < icounter){
					break;
				}

				for (j = 0; j < threadNumber; j++) {
					RandomValues rv = new RandomValues(RandomStringUtils.randomAlphanumeric(32),
							RandomStringUtils.randomNumeric(32), RandomStringUtils.randomAlphanumeric(32),
							RandomStringUtils.randomAlphanumeric(32));

					AddMapEntriesThread athread = new AddMapEntriesThread(rv, f1Map, f2Map, mainMap);
					//AddMapEntriesThread athread = new AddMapEntriesThread(rv, f1Map, f2Map, mainMap,jedis);//ab jaake thread waali pojo class mei nya constructor bnaa
					executorService.execute(athread);
				}
				this.f1Map = f1Map;
				this.f2Map = f2Map;
				this.mainMap = mainMap;
			}
			executorService.shutdown();
			while (!executorService.isTerminated()) {
			}
			System.out.println("Exit Add Map Entries Threads");

		} catch (Exception e) {
			System.out.println("exception occured " + e);
		} finally {

			long totalTimeInMs = System.currentTimeMillis() - startTimeInMs;

			String totalTime = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(totalTimeInMs),
					TimeUnit.MILLISECONDS.toMinutes(totalTimeInMs)
							- TimeUnit.MINUTES.toMinutes(TimeUnit.MILLISECONDS.toHours(totalTimeInMs)),
					TimeUnit.MILLISECONDS.toSeconds(totalTimeInMs)
							- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalTimeInMs)));
			System.out.println("addMapEntriesThreads completed with mapEntriesCount[" + mapEntriesCount + "] threads["
					+ threadNumber + "] totalTime[" + totalTime + "]");
		}

		return null;
	}

	private Void addMapEntries(Map<String, String> f1Map, Map<String, Token2> f2Map, Map<String, String> mainMap) {
		System.out.println("Add Map Entries ");
		for (int i = 0; i < mapEntriesCount; i++) {
			String pan = RandomStringUtils.randomAlphanumeric(16);
			String seqnum = RandomStringUtils.randomNumeric(16);
			String gtoken = RandomStringUtils.randomAlphanumeric(16);
			String mtoken = RandomStringUtils.randomAlphanumeric(16);

			f1Map.put(pan, seqnum);
			f2Map.put(seqnum, new Token2(gtoken, mtoken));
			mainMap.put(pan, gtoken);

			if (printRandomMapEntries) {
				System.out
						.println("added pan[" + pan + "] seqnum[" + seqnum + "] gt[" + gtoken + "] m[t" + mtoken + "]");
			}

		}
		System.out.println("Exit Add Map Entries ");
		return null;
	}

	private Void syncMainMap(Map<String, String> f1Map, Map<String, Token2> f2Map, Map<String, String> mainMap) {
		Set<String> pans = f1Map.keySet();
		Iterator<String> iter = pans.iterator();
		long syncCtr = 0;

		while (iter.hasNext()) {
			String pan = iter.next();
			String gtoken = mainMap.get(pan);
			if (null != gtoken && gtoken.isEmpty()) {
				Token2 tok = f2Map.get(pan);
				if (null != tok) {
					String gtok = tok.getGt();
					if (null != gtok && !gtok.isEmpty()) {
						mainMap.put(pan, gtok);
						syncCtr++;
					}
				}
			}
		}
		if (printSyncCounter)
			System.out.println("**** sync counter[" + syncCtr + "]");
		return null;
	}

	private boolean loadF1(Map<String, String> map) throws JsonProcessingException, IOException {

		// Reading F1.csv
		CsvMapper mapper = new CsvMapper();
		CsvSchema schema = CsvSchema.builder().addColumn(SEQKEY).addColumn(PAN).build();

		MappingIterator<Map<String, String>> iterator = mapper.reader(Map.class).with(schema).readValues(f1File);

		iterator.readAll().stream().parallel().forEach((k) -> {

			String seqkey = k.get(SEQKEY);
			String pan = k.get(PAN);

			// check if pan is present or not
			if (pan.equals("")) {
			} else {

				map.put(pan, seqkey);
				if (printsysout)
					System.out.println(PAN + pan + SEQKEY + seqkey);

			}
		});
		return true;
	}

	public boolean loadF2(Map<String, Token2> map) throws JsonProcessingException, IOException {

		// Reading F2.csv
		CsvMapper mapper = new CsvMapper();
		CsvSchema schema = CsvSchema.builder().addColumn(SEQKEY).addColumn(GT).addColumn(MT).build();

		MappingIterator<Map<String, String>> iterator = mapper.readerFor(Map.class).with(schema).readValues(f2File);

		iterator.readAll().stream().parallel().forEach((k) -> {

			String seqkey = k.get(SEQKEY);
			String gt = k.get(GT);
			String mt = k.get(MT);

			Token2 token = new Token2(gt, mt);

			map.put(seqkey, token);
			if (printsysout)
				System.out.println("f2 put seqkey " + seqkey + "  token " + token);

		});

		return true;
	}

	public boolean loadMap(Map<String, String> f1Map, Map<String, Token2> f2Map, Map<String, Token2> mainMap) {

		int nulltokenctr = 0;

		for (Map.Entry<String, String> entry : f1Map.entrySet()) {

			String pan = entry.getKey();
			String f1SeqKey = entry.getValue();

			Token2 token = f2Map.get(f1SeqKey);

			if (token != null) {

				mainMap.put(pan, token);
				if (printsysout)
					System.out.println(PAN + pan + " token " + token);

				/*
				 * try { Token2 tokenReturned = asyncFuture.get(1000,
				 * TimeUnit.MILLISECONDS); Token2 tokenReturned =
				 * asyncFuture.get();
				 * 
				 * if (printsysout) System.out.println("pan " + pan +
				 * " token returned " + tokenReturned); } catch
				 * (InterruptedException e) { System.out.println(
				 * "token could not be retrived for pan " + pan +
				 * ". exception occurred " + e ); e.printStackTrace(); } catch
				 * (ExecutionException e) { System.out.println(
				 * "token could not be retrived for pan " + pan +
				 * ". exception occurred " + e ); e.printStackTrace(); }
				 * catch(TimeoutException e) { System.out.println(
				 * "token could not be retrived for pan " + pan +
				 * ". exception occurred " + e ); } } else { nulltokenctr++;
				 * System.out.println(
				 * "f2map returned null token, cannot add to main map"); }
				 */
			}
		}
		System.out.println("f2map returned " + nulltokenctr + " null tokens");
		return true;
	}

	public void writeToFile(Map<String, Token2> mainMap) throws IOException {

		FileWriter fileWriter = new FileWriter(outputFile);
		CSVWriter csvWriter = new CSVWriter(fileWriter);

		for (Map.Entry<String, Token2> entry : mainMap.entrySet()) {

			String pan = entry.getKey();
			Token2 token = entry.getValue();

			if (token != null) {
				csvWriter.writeNext(new String[] { pan, token.getGt(), token.getMt() });

			}
		}

		csvWriter.flush();
		csvWriter.close();

	}

	public MappingIterator<Map<String, String>> loadF1File() throws JsonProcessingException, IOException {

		// Reading F1.csv
		CsvMapper mapper = new CsvMapper();
		CsvSchema schema = CsvSchema.builder().addColumn(SEQKEY).addColumn(PAN).build();
		System.out.print("Entered Mapping Iterator");
		return mapper.reader(Map.class).with(schema).readValues(f1File);
	}

	@Override
	public Void call() throws InterruptedException, IOException {
		int counter = 0;

		String startdttime = LocalDateTime.now().format(formatter);
		System.out.println("--------------- PROCESS START TIME  ------------------- " + startdttime);

		try {

			   /*Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		        jedisClusterNodes.add(new HostAndPort("10.0.0.4", 6379));
		        jedisClusterNodes.add(new HostAndPort("10.0.0.12", 6379));
		      
		    System.out.println("cluster node set initialised");
		    jedis = new JedisCluster(jedisClusterNodes);
		    System.out.println("this line is after creation of jedisCluster");*/
			//jedis = new Jedis("10.0.0.12");
			List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();

		    JedisShardInfo si = null;

		    si = new JedisShardInfo("10.0.0.4", 6379);
		    shards.add(si);
		    si = new JedisShardInfo("10.0.0.12", 6379);
		    shards.add(si);
		    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		    ShardedJedisPool pool = new ShardedJedisPool(jedisPoolConfig, shards);

		    jedis = pool.getResource();
			//jedis = new ShardedJedis(shards);
		    System.out.println("pohonche kya shard setup tak????");
		  
			System.out.println("F1MapSize" + f1Map.size());

			if (addMapEntries) {
				addMapEntriesThreads(f1Map, f2Map, mainMap);
				putIntoCache(mainMap);
				return null;
			}
			// get iterator to file
			loadF1(f1Map);
			loadF2(f2Map);

			MappingIterator<Map<String, String>> iterator = loadF1File();

			iterator.readAll().stream().parallel().forEach((k) -> {

				String seqkey = k.get(SEQKEY);
				String pan = k.get(PAN);

				// check if pan is present or not
				if (null != pan && !pan.isEmpty() && null != seqkey && !seqkey.isEmpty()) {

					Token2 token;
					try {
						// token = f2Map.get(pan);
						token = f2Map.get(seqkey);
						if (null != token) {

							String globalt = token.getGt();
							if (null != globalt && !globalt.isEmpty()) {
								mainMap.put(pan, globalt);

							}
						}

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			putIntoCache(mainMap);
		} catch (Exception ioe) {
			ioe.printStackTrace();

		} finally {
			jedis.close();
			String enddttime = LocalDateTime.now().format(FORMATTER);
			System.out.println("--------------- PROCESS STARTED ON ------------------- " + startdttime);
			System.out.println("--------PROCESS ENDED ON---------------- " + enddttime);
		}

		return null;
	}

	public Void call2() throws InterruptedException {

		String startdttime = LocalDateTime.now().format(formatter);
		System.out.println("--------------- PROCESS START TIME  ------------------- " + startdttime);

		try {

			// hz = HazelcastClient.newHazelcastClient();

			Map<String, String> f1Map = new HashMap<String, String>();

			System.out.println("--------------- loading f1map - start  ------------------- "
					+ LocalDateTime.now().format(FORMATTER));
			boolean f1loaded = loadF1(f1Map);
			System.out.println(
					"--------------- loaded f1map - end  ------------------- " + LocalDateTime.now().format(FORMATTER));

			if (f1loaded) {

				Map<String, Token2> f2Map = new HashMap<String, Token2>();
				System.out.println("--------------- loading f2map - start  ------------------- "
						+ LocalDateTime.now().format(FORMATTER));
				boolean f2loaded = loadF2(f2Map);
				System.out.println("--------------- loaded f2map - end ------------------- "
						+ LocalDateTime.now().format(FORMATTER));

				if (f2loaded) {

					System.out.println("--------------- loading mainmap - start ------------------- "
							+ LocalDateTime.now().format(FORMATTER));

					Map<String, Token2> mainMap = new HashMap<String, Token2>();
					boolean mainloaded = loadMap(f1Map, f2Map, mainMap);
					System.out.println("--------------- loaded mainmap - end ------------------- "
							+ LocalDateTime.now().format(FORMATTER));

					/*
					 * if (mainloaded) { System.out.println(
					 * "--------------- writing outfile - start  ------------------- "
					 * + LocalDateTime.now().format(formatter));
					 * writeToFile(mainMap); System.out.println(
					 * "--------------- writing outfile - end  ------------------- "
					 * + LocalDateTime.now().format(formatter));
					 */

				}
			}

		} catch (IOException ioe) {
		} finally {

			String enddttime = LocalDateTime.now().format(FORMATTER);
			System.out.println("--------------- PROCESS STARTED ON ------------------- " + startdttime);
			System.out.println("--------PROCESS ENDED ON---------------- " + enddttime);
		}

		return null;
	}

	public Void putIntoCache(Map<String, String> map) {
		String startdttime = LocalDateTime.now().format(formatter);
		System.out.println("--------------- Caching STARTED ON ------------------- " + startdttime);

		for (Map.Entry<String, String> entry : map.entrySet()) {
			jedis.set(entry.getKey(), entry.getValue());

		}

		String enddttime = LocalDateTime.now().format(FORMATTER);
		System.out.println("--------Caching ENDED ON---------------- " + enddttime);
		return null;

	}
}
