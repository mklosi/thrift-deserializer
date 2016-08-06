import com.google.common.base.Charsets;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.thrift.TBase;

/**
 * created by mklosi on 7/9/16.
 */
public class ThriftSimpleInputFormat extends FileInputFormat<NullWritable, RawEvent> {

    @Override
    public RecordReader<NullWritable, RawEvent> createRecordReader(InputSplit split,
                                                                   TaskAttemptContext context) {
        return new ThriftSimpleRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

}
