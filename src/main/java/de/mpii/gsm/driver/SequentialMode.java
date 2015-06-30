package de.mpii.gsm.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import de.mpii.gsm.localmining.Dfs;
import de.mpii.gsm.taxonomy.NytTaxonomy;
import de.mpii.gsm.taxonomy.Taxonomy;
import de.mpii.gsm.utils.Dictionary;
import de.mpii.gsm.writer.SequentialGsmWwriter;

/**
 * @author Kaustubh Beedkar
 * 
 */
public class SequentialMode {

	private GsmConfig config;// = new GsmConfig();

	private Dfs gsm;

	private SequentialGsmWwriter writer;
	
	private Dictionary dictionary;

	public SequentialMode(GsmConfig config) throws IOException {
		this.config = config;

		if (config.isKeepFiles()) {
			try {
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);

				// create output files
				String inputFile = config.getKeepFilesPath() + "/raw/part-r-00000";
				String dictionaryFile = config.getKeepFilesPath() + "/wc/part-r-00000";

				if (!fs.exists(new Path(inputFile)))
					fs.create(new Path(inputFile));
				else {
					fs.delete(new Path(inputFile), true);
					fs.create(new Path(inputFile));
				}
				if (!fs.exists(new Path(dictionaryFile)))
					fs.create(new Path(dictionaryFile));
				else{
					fs.delete(new Path(dictionaryFile),true);
					fs.create(new Path(dictionaryFile));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public void mine() {

	}

	

	private void processRecursively(File file) throws IOException, InterruptedException {
		if (file.isFile()) {
			encodeAndMineSequences(file);
		} else {
			File[] subdirs = file.listFiles();
			for (File subdir : subdirs) {
				if (subdir.isDirectory())
					processRecursively(subdir);
				else if (subdir.isFile())
					encodeAndMineSequences(subdir);
			}
		}
	}

	

	private void encodeAndMineSequences(File inputFile) throws IOException, InterruptedException {
		FileInputStream fstream = new FileInputStream(inputFile);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line;

		while ((line = br.readLine()) != null) {
			String[] items = line.split(config.getItemSeparator());

			int[] sequenceAsInts = new int[items.length - 1];

			// seqId item_1 item_2 ... item_n
			for (int i = 1; i < items.length; ++i) {
				sequenceAsInts[i - 1] = dictionary.getTids().get(items[i].trim());
			}
			gsm.addTransaction(sequenceAsInts, 0, sequenceAsInts.length, 1);

			if (config.isKeepFiles()) {
				BufferedWriter bw = new BufferedWriter(new FileWriter(config.getKeepFilesPath() + "/raw/part-r-00000", true));
				for (int itemId : sequenceAsInts)
					bw.write(itemId + " ");

				bw.write("\n");
				bw.close();
			}
		}
		br.close();

	}

	public void execute() throws Exception {

		this.writer = new SequentialGsmWwriter();

		if (config.isResume()) {
			String inputFile = config.getResumePath() + "/raw/part-r-00000";
			String dictionaryFile = config.getResumePath() + "/wc/part-r-00000";

			// Load dictionary
			Dictionary dict = new Dictionary(config);
			dict.load(null, dictionaryFile, config.getSigma());

			// Initialize writer
			writer.setItemIdToItemMap(dict.getItemIdToName());
			writer.setOutputPath(config.getOutputPath());

			// Initialize the taxonomy
			int[] itemToParent = dict.getItemToParent();
			Taxonomy taxonomy = new NytTaxonomy(itemToParent);

			// Mining
			gsm = new Dfs();
			gsm.setParameters(config.getSigma(), config.getGamma(), config.getLambda(), taxonomy);
			gsm.initialize();
			gsm.scanDatabase(inputFile);
			gsm.mine(writer);

		} else {
			// TODO: error checks for hierarchy path
			
			this.dictionary = new Dictionary(config);
			this.dictionary.createDictionaryFromSequenceFiles(config.getHierarchyPath(), config.getInputPath());

			if(config.isKeepFiles())
				dictionary.writeJSONDictionary(config.getKeepFilesPath().concat("/" + "wc/part-r-00000"));

			
			// Initialize writer
			writer.setItemIdToItemMap(this.dictionary.getItemIdToItemMap());
			writer.setOutputPath(config.getOutputPath());

			// Initialize taxonomy
			Taxonomy taxonomy = new NytTaxonomy(this.dictionary.getParentIds());

			// Mine sequences
			gsm = new Dfs();
			gsm.setParameters(config.getSigma(), config.getGamma(), config.getLambda(), taxonomy);
			gsm.initialize();
			processRecursively(new File(config.getInputPath()));
			
			
			// TODO: necessary?
			dictionary.clear();

			gsm.mine(writer);

		}

	}

}
