package hadoop.model;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import java.io.Serializable;

public class StringArrayWritable extends ArrayWritable implements Serializable {

    public StringArrayWritable() {
            super(Text.class);
    }

    public StringArrayWritable(String[] strings) {
        super(Text.class);

        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new Text(strings[i]);
        }
        set(texts);
    }
}


