package com.redgate.hadoop.hive.azuretables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.microsoft.windowsazure.services.table.client.EntityProperty;

/**
 * Hadoop WriteableComparable wrapper for an Azure Tables entity
 * 
 * @author Simon Elliston Ball <simon@simonellistonball.com>
 * 
 */
public class EntityPropertyWriteable implements
		WritableComparable<EntityPropertyWriteable> {

	private EntityProperty property;

	public EntityPropertyWriteable(EntityProperty property) {
		this.property = property;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub

	}

	public int compareTo(EntityPropertyWriteable other) {
		if (other.getProperty().getEdmType() == this.getProperty().getEdmType()) {
			return compareBytes(this.getProperty().getValueAsByteArray(), other
					.getProperty().getValueAsByteArray());
		} else {
			return -1;
		}
	}

	public EntityProperty getProperty() {
		return property;
	}

	public void setProperty(EntityProperty property) {
		this.property = property;
	}

	/**
	 * Comparison of byte arrays
	 * 
	 * TODO there is probably a better way to do this with Hadoop.
	 * 
	 * from http://stackoverflow.com/questions/5108091/java-comparator-for-byte-
	 * array-lexicographic
	 * 
	 * @param left
	 * @param right
	 * @return
	 */
	private int compareBytes(byte[] left, byte[] right) {
		for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
			int a = (left[i] & 0xff);
			int b = (right[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return left.length - right.length;
	}

}
