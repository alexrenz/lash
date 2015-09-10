package de.mpii.gsm.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;



/**
 * Writable that represents a value for an item key with item Frequency and Parent information.
 * 
 * @author kbeedkar
 *
 */
public class FPWritable implements WritableComparable<FPWritable>{
	
	private int frequency = 0;
	private String[] ancestors = null;
	
	public FPWritable(){
		
	}
	
	public FPWritable(int frequency, String[] ancestors) {
		this.frequency = frequency;
		this.ancestors = ancestors;
	}

	@Override
	public void readFields(DataInput di) throws IOException {
		frequency = WritableUtils.readVInt(di);
		ancestors = WritableUtils.readStringArray(di);
	}

	@Override
	public void write(DataOutput d) throws IOException {
		WritableUtils.writeVInt(d, frequency);
		WritableUtils.writeStringArray(d, ancestors);
	}

	@Override
	public int compareTo(FPWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public int getFrequency(){
		return frequency;
	}
	
	public String[] getAncestors(){
		return ancestors;
	}
	
	public void setFrequency(int frequency){
		this.frequency = frequency;
	}
	
	public void setAncestors(String[] ancestors) {
		this.ancestors = ancestors;
	}
	
	
	public static void main(String[] args) {
		
		
		FPWritable w = new FPWritable();
		w.setFrequency(10);
		w.setAncestors(new String[] {"abcd", "bcde"});
		
		System.out.println(w.getFrequency() + " " + w.getAncestors());
		
		
		w.setFrequency(12341234);
		w.setAncestors(null);
		
		System.out.println(w.getFrequency() + " " + w.getAncestors());
		
		if(w.getAncestors() == null){
			System.out.println("null value");
		}
		
	}

}
