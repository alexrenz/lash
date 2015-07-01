package de.mpii.gsm.utils;


import de.mpii.gsm.driver.GsmConfig;
import de.mpii.gsm.utils.PrimitiveUtils;
import de.mpii.gsm.utils.DirectedGraph;
import de.mpii.gsm.utils.TopologicalSort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

public class Dictionary {
	
	//Combines itemId and support value in a long value
	protected ArrayList<Long> items = new ArrayList<Long>();
	
	public int[] parentsListPositions;
	public int[] parentsList;
	
	protected OpenIntObjectHashMap<String> itemIdToItemMap = new OpenIntObjectHashMap<String>();

	protected OpenObjectIntHashMap<String> tids = new OpenObjectIntHashMap<String>();
	
	final OpenObjectIntHashMap<String> cfs = new OpenObjectIntHashMap<String>();
	final OpenObjectIntHashMap<String> dfs = new OpenObjectIntHashMap<String>();

	private HashMap<String, ArrayList<String>> parents = new HashMap<String, ArrayList<String>>();
	
	private HashMap<String, Integer> topologicalOrder = new HashMap<String, Integer>();
	
	private GsmConfig config;
	
	public Dictionary() {
		this.config = new GsmConfig();
	}
	
	public Dictionary(GsmConfig config) {
		this.config = config;
	}
	
	public OpenObjectIntHashMap<String> getTids() {
		return tids;
	}

	public OpenIntObjectHashMap<String> getItemIdToItemMap() {
		return itemIdToItemMap;
	}
	
	public void load(Configuration conf, String fileName, int minSupport) throws IOException {
		
		BufferedReader br = null;
		if (conf == null) {
			@SuppressWarnings("resource")
			FileInputStream fstream = new FileInputStream(fileName);
			// Get the object of DataInputStream
			DataInputStream in = new DataInputStream(fstream);
			br = new BufferedReader(new InputStreamReader(in));
		} else {
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream dis = fs.open(new Path(fileName));
			br = new BufferedReader(new InputStreamReader(dis));
		}
		
		OpenIntIntHashMap parentMap = new OpenIntIntHashMap();
		
		String line = null;

		while ((line = br.readLine()) != null) {
			String[] splits = line.split("\t");
			int itemId = Integer.parseInt(splits[3]);
			int itemSupport = Integer.parseInt(splits[2]);
			
			int parentId = Integer.parseInt(splits[4]);
			
			parentMap.put(itemId, parentId);

			if (itemSupport >= minSupport) {
				items.add(PrimitiveUtils.combine(itemId, itemSupport));
			}
			itemIdToItemMap.put(itemId, splits[0]);
		}
		
		Collections.sort(items, new MyComparator());
		
		System.out.println("ERROR: dictionary load does not work at the moment");
		System.exit(1);
		/* TODO: convert to new data structure
		parentIds = new int[parentMap.size() + 1];
		IntArrayList keyList = parentMap.keys();
		for(int i = 0; i < keyList.size(); ++i) {
			int item = keyList.get(i); 
			parentIds[item] = parentMap.get(item);
		}
		*/ 
	}

	public void createDictionaryFromSequenceFiles(String sequenceFilesPath, String hierarchyPath) {
		// Process the hierarchy
		File hFile = new File(sequenceFilesPath);
		processHierarchy(hFile);

		// Read the input files to create the f-list
		File iFile = new File(hierarchyPath);
		processRecursively(iFile);
		
		List<String> temp = cfs.keys();
		String[] terms = Arrays.copyOf(temp.toArray(), temp.toArray().length, String[].class);

		// sort terms in descending order of their collection frequency and topological order
		Arrays.sort(terms, new Comparator<String>() {
			@Override
			public int compare(String t, String u) {
				if(cfs.get(u) - cfs.get(t) != 0)
					// 'larger' cfs first
					return cfs.get(u) - cfs.get(t);
				else
					// 'smaller' order first
					return  topologicalOrder.get(t) - topologicalOrder.get(u);
			}
		});

		// assign term identifiers
		for (int i = 0; i < terms.length; i++) {
			tids.put(terms[i], (i + 1));
		}

		
		// create ItemId to item Map
		itemIdToItemMap = new OpenIntObjectHashMap<String>();
		for (String term : terms) {
			itemIdToItemMap.put(tids.get(term), term);
		}

		// Store parents in two-array data structure: position list and parent list
		IntArrayList tempParentsList = new IntArrayList();
		int currentPosition = 0;
		parentsListPositions = new int[terms.length+2]; // +1 as tids start with 1, other +1 for dummy element at the end
		
		for (int i=1; i<terms.length+1; i++) {
			parentsListPositions[i] = currentPosition;
			String term = terms[i-1];
			if(parents.containsKey(term)) {
				for(String parent : parents.get(term)) {
					tempParentsList.add(tids.get(parent));
					currentPosition++;
				}
			}
		}
		parentsList = tempParentsList.toArray(new int[tempParentsList.size()]);
		
		// Add dummy item at the end of the positions list to make access easier for the last item
		parentsListPositions[parentsListPositions.length - 1] = parentsList.length;
		
	}
	
	private void processRecursively(File file) {
		if (file.isFile()) {
			scanSequences(file);
		} else {
			File[] subdirs = file.listFiles();
			for (File subdir : subdirs) {
				if (subdir.isDirectory())
					processRecursively(subdir);
				else if (subdir.isFile())
					scanSequences(subdir);
			}
		}
	}
	
	private void scanSequences(File inputFile) {

		try {
			FileInputStream fstream = new FileInputStream(inputFile);
		
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
	
			String line;
	
			// compute generalized f-list
			while ((line = br.readLine()) != null) {
				OpenObjectIntHashMap<String> wordCounts = new OpenObjectIntHashMap<String>();
	
				String[] items = line.split(config.getItemSeparator());
	
				// seqId item_1 item_2 ... item_n
				for (int i = 1; i < items.length; ++i) {
					String item = items[i].trim();
	
					wordCounts.adjustOrPutValue(item, +1, +1);
					
					// TODO: increase count for multiple parents
					// [temporarily, only first parent considered]
					while (parents.get(item) != null) {
						wordCounts.adjustOrPutValue(parents.get(item).get(0), +1, +1);
						item = parents.get(item).get(0);
					}
				}
	
				for (String item : wordCounts.keys()) {
					cfs.adjustOrPutValue(item, +wordCounts.get(item), +wordCounts.get(item));
					dfs.adjustOrPutValue(item, +1, +1);
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void processHierarchy(File hFile) {
		try {
			FileInputStream fstream = new FileInputStream(hFile);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String line;
			
			DirectedGraph<String> parentsGraph = new DirectedGraph<String>();

			while ((line = br.readLine()) != null) {
				String[] splits = line.split(config.getItemSeparator());
				if (splits.length == 2) {
					String item = splits[0].trim();
					String parent = splits[1].trim();
					if(!parents.containsKey(splits))
						parents.put(item, new ArrayList<String>());
					parents.get(item).add(parent);
					
					parentsGraph.addNode(item);
					parentsGraph.addNode(parent);
					parentsGraph.addEdge(parent, item);
				}
			}
			br.close();
			
			// Do a topological sort and store inverted list
			// also: detect cycles in the hierarchy
			List<String> sorted = null;
			try {
				sorted = TopologicalSort.sort(parentsGraph);
			} catch (IllegalArgumentException e) {
				System.out.println("The given hierarchy contains a cycle. Exiting...");
				// TODO: check whether this exit works in distributed mode
				System.exit(1);
			} 
			for(int i=0; i<sorted.size(); i++) {
				topologicalOrder.put(sorted.get(i), i);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void writeJSONDictionary(String writePath) {
		// TODO
		// Write dictionary
		/* old code from Sequentialmode.java
		if (config.isKeepFiles()) {
			String outputFileName = config.getKeepFilesPath();

			File outFile = new File(outputFileName.concat("/" + "wc/part-r-00000"));

			try {

				OutputStream fstreamOutput = new FileOutputStream(outFile);

				// Get the object of DataOutputStream
				DataOutputStream out = new DataOutputStream(fstreamOutput);
				BufferedWriter br1 = new BufferedWriter(new OutputStreamWriter(out));

				// Perform the writing to the file
				for (String term : terms) {
					int parentId = (parents.get(term) == null) ? 0 : tids.get(parents.get(term));

					br1.write(term + "\t" + cfs.get(term) + "\t" + dfs.get(term) + "\t" + tids.get(term) + "\t" + parentId + "\n");
				}
				br1.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} */
	}


	public void clear() {
		tids.clear();
		parents.clear();
		parentsListPositions = null;
		parentsList = null;
	}
	
	public OpenIntObjectHashMap<String> getItemIdToName(){
		return itemIdToItemMap;
	}
	
	//Items are sorted in decreasing order of frequencies
	public ArrayList<Long> getItems(){
		return items;
	}
	
	
	public int[] getItemToParent(){
		//TODO-dm: remove this method
		//return parentIds;
		System.out.println("getItemToParent not implemented currently. Exiting...");
		System.exit(1);
		return new int[1];
	}  
	
	public static void main(String[] args) throws IOException {
	}

}
