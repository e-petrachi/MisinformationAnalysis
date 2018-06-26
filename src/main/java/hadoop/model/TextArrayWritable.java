package hadoop.model;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.commons.nullanalysis.NotNull;
import spark.analysis.AllInOne;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

public class TextArrayWritable extends ArrayWritable implements Serializable {

    private static final Logger LOG = Logger.getLogger(TextArrayWritable.class);
    static { LOG.setLevel(Level.DEBUG);}

    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(final Text... elements) {
        super(Text.class, elements);
    }

    public TextArrayWritable(final String[] strings) {
        super(Text.class, toText(strings));

    }
    private static Text[] toText(final String[] strings){
        if (strings == null)
            return null;

        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new Text(strings[i]);
        }
        return texts;
    }

    @Override
    public Text[] get() {
        final Writable[] writables = super.get();
        if (writables == null)
            return null;

        Text[] texts = new Text[writables.length];
        for (int i = 0; i < writables.length; ++i) {
            texts[i] = (Text) writables[i];
        }
        return texts;
    }

    @Override
    public String toString() {
        return Arrays.toString(get());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (get() == null)
            return;

        for (Text t : get()){
            t.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (get() == null)
            return;

        for (Text t : get()){
            t.readFields(in);
        }
    }
}


