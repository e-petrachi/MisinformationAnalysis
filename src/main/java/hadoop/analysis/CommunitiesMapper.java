package hadoop.analysis;

import hadoop.model.TextArrayWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.ArrayList;

public class CommunitiesMapper extends Mapper<Object, BSONObject, Text, ArrayWritable> {

    private Text chiave;
    private TextArrayWritable valore;

    private static final Logger LOG = Logger.getLogger(CommunitiesMapper.class);
    static { LOG.setLevel(Level.INFO);}

    public void map(Object key, BSONObject val, Context context)
            throws IOException, InterruptedException {

        String keyOut = (String) val.get("hashtag");
        ArrayList<String> valOut = (ArrayList<String>) val.get("users");
        String[] out = valOut.toArray(new String[valOut.size()]);

        this.chiave = new Text(keyOut);
        this.valore = new TextArrayWritable(out);

        context.write(this.chiave, this.valore);

        LOG.debug("* MAPPER_KEY: " + this.chiave.toString() + " * MAPPER_VALUE: " + valOut.toString());

    }
}
