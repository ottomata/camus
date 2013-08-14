package  org.wikimedia.analytics.kraken.etl;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 *
 *
 */
public class TextRecordWriterProvider implements RecordWriterProvider {
    // TODO: Make this configurable
    public final static String EXT = ".txt";

    @Override
    public String getFilenameExtension() {
        return EXT;
    }

    @Override
    public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(
            TaskAttemptContext context,
            String fileName,
            CamusWrapper data,
            FileOutputCommitter committer) throws IOException, InterruptedException {


        Path path = new Path(
            committer.getWorkPath(),
            EtlMultiOutputFormat.getUniqueFile(context, fileName, EXT)
        );

        final FSDataOutputStream writer = path.getFileSystem(
            context.getConfiguration()
        ).create(path);

        return new RecordWriter<IEtlKey, CamusWrapper>() {
            @Override
            public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
                String record = ((String)data.getRecord() + "\n");
                writer.write(record.getBytes());
            }

            @Override
            public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
                writer.close();
            }
        };
    }
}
