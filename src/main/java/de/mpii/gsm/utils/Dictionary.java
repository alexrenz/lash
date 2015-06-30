package de.mpii.gsm.utils;


import de.mpii.gsm.driver.GsmConfig;
import de.mpii.gsm.utils.PrimitiveUtils;

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
	
	protected int[] parentIds;
	
	protected OpenIntObjectHashMap<String> itemIdToItemMap = new OpenIntObjectHashMap<String>();

	protected OpenObjectIntHashMap<String> tids = new OpenObjectIntHashMap<String>();
	
	final OpenObjectIntHashMap<String> cfs = new OpenObjectIntHashMap<String>();
	final OpenObjectIntHashMap<String> dfs = new OpenObjectIntHashMap<String>();

	private HashMap<String, String> parents = new HashMap<String, String>();
	
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
		// TODO: revisit - good way to do this over get method (used in SequentialMode.java)
		return itemIdToItemMap;
	}
	
	public int[] getParentIds() {
		// TODO: revisit - good way to do this?
		return parentIds;
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
		
		parentIds = new int[parentMap.size() + 1];
		IntArrayList keyList = parentMap.keys();
		for(int i = 0; i < keyList.size(); ++i) {
			int item = keyList.get(i); 
			parentIds[item] = parentMap.get(item);
		}
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

		// Remove parents with same frequency as children
		for (int i = 0; i < terms.length; ++i) {
			String term = terms[i];
			String parent = parents.get(term);
			if (term == null || parent == null)
				continue;
			while (cfs.get(term) == cfs.get(parent)) {
				parents.put(term, parents.get(parent));
				parent = parents.get(parent);
				if (parent == null)
					break;
			}
		}

		// sort terms in descending order of their collection frequency
		Arrays.sort(terms, new Comparator<String>() {
			@Override
			public int compare(String t, String u) {
				return cfs.get(u) - cfs.get(t);
			}
		});

		// assign term identifiers
		for (int i = 0; i < terms.length; i++) {
			tids.put(terms[i], (i + 1));
		}

		

		// create ItemId to ParentId list
		parentIds = new int[terms.length + 1];
		itemIdToItemMap = new OpenIntObjectHashMap<String>();

		for (String term : terms) {
			int parentId = (parents.get(term) == null) ? 0 : tids.get(parents.get(term));
			parentIds[tids.get(term)] = parentId;

			itemIdToItemMap.put(tids.get(term), term);
		}
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
	
					while (parents.get(item) != null) {
						wordCounts.adjustOrPutValue(parents.get(item), +1, +1);
						item = parents.get(item);
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

			while ((line = br.readLine()) != null) {
				String[] splits = line.split(config.getItemSeparator());
				if (splits.length == 2)
					parents.put(splits[0].trim(), splits[1].trim()); // TODO: check for DAGs
			}
			br.close();
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
		// TODO: check where this function is used and whether it makes sense. (copied from SequentialMode.java)
		tids.clear();
		parents.clear();
	}
	
	public OpenIntObjectHashMap<String> getItemIdToName(){
		return itemIdToItemMap;
	}
	
	//Items are sorted in decreasing order of frequencies
	public ArrayList<Long> getItems(){
		return items;
	}
	
	
	public int[] getItemToParent(){
		return parentIds;
	}
	
	public static void main(String[] args) throws IOException {
	}

}
