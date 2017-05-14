package project.MR;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

// DMO cass to maintain the order of the unlabelled data in the results of prediction
public class IndexKeyWritable implements WritableComparable<IndexKeyWritable> {

	private long taskID;
	private int localIndex;
	private String SamplingID;

	public IndexKeyWritable() {

	}

	public IndexKeyWritable(long tid, int lindex, String sID) {
		taskID = tid;
		localIndex = lindex;
		SamplingID = sID;
	}

	public String getSamplingID() {
		return SamplingID;
	}

	public long gettaskID() {
		return taskID;
	}

	public int getlocalIndex() {
		return localIndex;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(taskID);
		out.writeInt(localIndex);
		out.writeUTF(SamplingID);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		taskID = in.readLong();
		localIndex = in.readInt();
		SamplingID = in.readUTF();
	}

	// Function to compare the task results with that of the unlabelled data
	// order.
	@Override
	public int compareTo(IndexKeyWritable obj) {
		// if(this.taskID == obj.taskID)
		int cmp = (new Long(this.taskID)).compareTo(new Long(obj.taskID));
		if (0 != cmp)
			return cmp;
		cmp = (localIndex < obj.localIndex) ? -1 : (localIndex == obj.localIndex) ? 0 : 1;
		return cmp;

	}

}
