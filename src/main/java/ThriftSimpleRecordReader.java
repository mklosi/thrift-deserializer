/**
 * created by mklosi on 7/9/16.
 */

import java.io.BufferedInputStream;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;


public class ThriftSimpleRecordReader extends RecordReader<NullWritable, RawEvent> {

    int count;
    private FSDataInputStream fileIn;
    TBinaryProtocol binaryIn;
    RawEvent rawEvent;

    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {

        FileSplit split = (FileSplit) genericSplit;

        long length;
        SplitLocationInfo[] locInfo;
        String[] locs;
        final Path file = split.getPath();
        Configuration hconf = context.getConfiguration();
        long start;

        length = split.getLength();
        locInfo = split.getLocationInfo();
        locs = split.getLocations();
        start = split.getStart();

        //-----------------
        float progress;
        String status;
        TaskAttemptID taskAttemptID;

        progress = context.getProgress();
        status = context.getStatus();
        taskAttemptID = context.getTaskAttemptID();

        //------------------------

//        InputStream inputStream = new BufferedInputStream(new FileInputStream(file), 2048);
//        TBinaryProtocol binaryIn = new TBinaryProtocol(new TIOStreamTransport(inputStream));

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(hconf);
        fileIn = fs.open(file);

        fileIn.seek(start); // ^^^ necessary?

        FileDescriptor fileDescriptor = fileIn.getFileDescriptor();
        long pos = fileIn.getPos();

        // ^^^ let's believe that this is actually buffered
        // ^^^ try with TMemoryInputTransport - it's not working, since TMemoryInputTransport needs byte[]
        // TIOStreamTransport
        binaryIn = new TBinaryProtocol(new TIOStreamTransport(fileIn.getWrappedStream()));





        // ---------------------
        int debug = 1;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public RawEvent getCurrentValue() {
//        RawEvent rawEvent = new RawEvent();
//        rawEvent.setTstampDveCreated(1234l);
//        rawEvent.setTstampDveSent(12345l);
//        rawEvent.setTstampCorReceived(123456l);
//        rawEvent.setDataType("dttype");
//        rawEvent.setDataSource("dtsource");
//        rawEvent.setEventId("eventId-" + count);
//        rawEvent.setEventType("eventType-1");
//        rawEvent.setEventSource("eventSource-1");
        return rawEvent;
    }

    public boolean nextKeyValue() throws IOException {
        if (hasNext()) {
            rawEvent = new RawEvent(); // ^^^ considered using reusable?
            try {
                rawEvent.read(binaryIn);
            } catch (TException e) {
                e.printStackTrace();
            }
            count++;
            return true;
        }

        rawEvent = null;
        return false;
    }

    private boolean hasNext() throws IOException {
        long pos = fileIn.getPos();
        int checker = fileIn.read();
        fileIn.seek(pos);
        return checker != -1;
    }

    /**
     * Get the progress within the split
     */
    public float getProgress() throws IOException {
        return count;
    }

    public synchronized void close() throws IOException {
        if (fileIn != null) {
            fileIn.close();
        }
    }
}
