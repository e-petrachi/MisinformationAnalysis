package hadoop.analysis;

import hadoop.model.StringArrayWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;


public class CommunitiesReducer extends
        Reducer<Text, StringArrayWritable, Text, IntWritable> {

    private static final Logger LOG = Logger.getLogger(CommunitiesReducer.class);
    static { LOG.setLevel(Level.INFO);}

    private IntWritable valore;

    public void reduce(Text key, Iterator<StringArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        StringArrayWritable tmp;

        while (values.hasNext()) {
            tmp = values.next();
        }

        this.valore =  new IntWritable(1);

        context.write(key, this.valore);

        LOG.debug("* REDUCER_KEY: " + key + " * REDUCER_VALUE: " + this.valore);
    }

}
