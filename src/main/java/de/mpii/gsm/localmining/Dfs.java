package de.mpii.gsm.localmining;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import de.mpii.gsm.utils.Dictionary;
import de.mpii.gsm.writer.GsmWriter;

/**
 * @author Kaustubh Beedkar (kbeedkar@uni-mannheim.de)
 *
 */
public class Dfs {
	
	protected int sigma;
	protected int gamma;
	protected int lambda;

	// List of seen transactions
	protected ArrayList<int[]> inputTransactions = new ArrayList<int[]>();

	// Support count for all the transactions in inputTransactions
	protected IntArrayList transactionSupports = new IntArrayList();
	
	protected Dictionary dictionary;

	private int _noOfFrequentPatterns = 0;

	protected int beginItem = 0;
	protected int endItem = Integer.MAX_VALUE;

	protected Items globalItems = new Items();

	private int[] transaction = null;
	GsmWriter writer;

	public Dfs() {
	}

	public Dfs(int sigma, int gamma, int lambda, Dictionary dictionary) {
		this.sigma = sigma;
		this.gamma = gamma;
		this.lambda = lambda;
		this.dictionary = dictionary;
	}

	public void clear() {
		inputTransactions.clear();
		transactionSupports.clear();
		globalItems.clear();
	}

	public void setParameters(int sigma, int gamma, int lambda, Dictionary dictionary) {
		this.sigma = sigma;
		this.gamma = gamma;
		this.lambda = lambda;
		this.dictionary = dictionary;
		clear();
	}

	public void initialize() {
		initialize(0, Integer.MAX_VALUE);
	}

	public void initialize(int b, int e) {
		clear();
		this.beginItem = b;
		this.endItem = e;
	}

	//TODO: is this method used anywhere?
	// yep, there is one in SequentialMode.java
	public void scanDatabase(String dbFile) throws Exception {

		FileInputStream fstream;
		fstream = new FileInputStream(dbFile);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;

		while ((strLine = br.readLine()) != null) {
			if (!strLine.isEmpty()) {
				String[] sequence = strLine.split("\\s* \\s*"); // TODO: take item
				// separator as
				// parameter

				int[] sequenceAsInts = new int[sequence.length];
				int i = 0;
				for (String term : sequence) {
					sequenceAsInts[i] = Integer.parseInt(term);
					i++;
				}
				addTransaction(sequenceAsInts, 0, sequenceAsInts.length, 1);
			}
		}
		br.close();
	}

	/**
	 * Adds the specified subsequence of a given transaction to the collection of input transactions
	 * and adds items to the inverted list structure in globalItems
	 */
	public void addTransaction(int[] transaction, int fromIndex, int toIndex, int support) {
		int transactionId = transactionSupports.size();
		transactionSupports.add(support);

		int length = toIndex - fromIndex;
		int[] inputTransaction = new int[length];
		System.arraycopy(transaction, fromIndex, inputTransaction, 0, length);
		inputTransactions.add(inputTransaction);

		for (int i = fromIndex; i < toIndex; ++i) {
			assert transaction[i] <= endItem;
			if (transaction[i] < 0) {
				continue;
			}
			int itemId = transaction[i];
			
			// Update globalItems for this item
			globalItems.addItem(itemId, transactionId, support, i);
			
			// Also update globalItems for all the parents of this item
			for(int pos=dictionary.parentsListPositions[itemId]; 
					pos<dictionary.parentsListPositions[itemId+1]; 
					pos++) {
				globalItems.addItem(dictionary.parentsList[pos], transactionId, support, i);
			}
		}
	}

	/**
	 * Triggers the mining of the collected transactions.
	 */
	public void mine(GsmWriter writer) throws IOException, InterruptedException {
		this.writer = writer;
		_noOfFrequentPatterns = 0;

		int[] prefix = new int[1];

		for (Map.Entry<Integer, Item> entry : globalItems.itemIndex.entrySet()) {
			Item item = entry.getValue();
			if (item.support >= sigma) {
				prefix[0] = entry.getKey();
				dfs(prefix, item.transactionIds, (prefix[0] >= beginItem));
			}
		}
		clear();
	}

	/**
	 * Recursively executes patterns search in the search space
	 */
	private void dfs(int[] prefix, ByteArrayList transactionIds, boolean hasPivot) throws IOException,
			InterruptedException {
		if (prefix.length == lambda)
			return;
		PostingList.Decompressor transactions = new PostingList.Decompressor(transactionIds);

		Items localItems = new Items();

		do {
			int transactionId = transactions.nextValue();
			transaction = inputTransactions.get(transactionId);
			// for all positions
			while (transactions.hasNextValue()) {
				int position = transactions.nextValue();

				/** Add items in the right gamma+1 neighborhood */
				int gap = 0;
				for (int j = 0; gap <= gamma && (position + j + 1 < transaction.length); ++j) {
					int itemId = transaction[position + j + 1];
					if (itemId < 0) {
						gap -= itemId;
						continue;
					}
					gap++;
					if (globalItems.itemIndex.get(itemId).support >= sigma)
						localItems.addItem(itemId, transactionId, transactionSupports.get(transactionId), (position + j + 1));
					
					// Also consider parents of current item
					int parentId = 0;
					for(int pos=dictionary.parentsListPositions[itemId]; 
							pos<dictionary.parentsListPositions[itemId+1]; 
							pos++) {
						parentId = dictionary.parentsList[pos];
						if (globalItems.itemIndex.get(parentId).support >= sigma)
							localItems.addItem(parentId, transactionId, transactionSupports.get(transactionId), (position + j + 1));
						
					}
				}
			}

		} while (transactions.nextPosting());

		// Descend further
		int[] newPrefix = new int[prefix.length + 1];
		for (Map.Entry<Integer, Item> entry : localItems.itemIndex.entrySet()) {
			Item item = entry.getValue();
			if (item.support >= sigma) {
				System.arraycopy(prefix, 0, newPrefix, 0, prefix.length);
				newPrefix[prefix.length] = entry.getKey();

				boolean containsPivot = hasPivot || (newPrefix[prefix.length] >= beginItem);

				if (containsPivot) {
					_noOfFrequentPatterns++;
					if (writer != null)
						writer.write(newPrefix, item.support);
				}
				dfs(newPrefix, item.transactionIds, containsPivot);
			}
		}
		localItems.clear();
	}

	public int noOfFrequentPatterns() {
		return _noOfFrequentPatterns;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	// -- HELPER CLASSES --

	private static final class Item {

		int support;

		int lastTransactionId;

		int lastPosition;

		ByteArrayList transactionIds;

		Item() {
			support = 0;
			lastTransactionId = -1;
			lastPosition = -1;
			transactionIds = new ByteArrayList();
		}
	}

	private static final class Items {

		Int2ObjectOpenHashMap<Item> itemIndex = new Int2ObjectOpenHashMap<Item>();

		public void addItem(int itemId, int transactionId, int support, int position) {
			Item item = itemIndex.get(itemId);
			if (item == null) {
				item = new Item();
				itemIndex.put(itemId, item);
				// baseItems.add(itemId);
			}

			if (item.lastTransactionId != transactionId) {

				/** Add transaction separator */
				if (item.transactionIds.size() > 0) {
					PostingList.addCompressed(0, item.transactionIds);
				}
				item.lastPosition = position;
				item.lastTransactionId = transactionId;
				item.support += support;
				PostingList.addCompressed(transactionId + 1, item.transactionIds);
				PostingList.addCompressed(position + 1, item.transactionIds);

			} else if (item.lastPosition != position) {
				PostingList.addCompressed(position + 1, item.transactionIds);
				item.lastPosition = position;
			}
		}

		public void clear() {
			itemIndex.clear();
		}
	}
}
