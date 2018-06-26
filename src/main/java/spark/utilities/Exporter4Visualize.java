package spark.utilities;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jongo.MongoCursor;
import spark.model.post_model.CommunitiesHashtag;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

public class Exporter4Visualize {

    private static final Logger LOG = Logger.getLogger(Exporter4Visualize.class);
    static { LOG.setLevel(Level.INFO);}

    public static void main(String[] args) throws IOException {
        MongoLocal mongoLocal = new MongoLocal();
        mongoLocal.setDbName("bigdata");
        mongoLocal.setCollection("communitiesHashtag");

        MongoCursor<CommunitiesHashtag> communitiesHashtags = mongoLocal.findAllCommunitiesHashtag();

        writeCsv4Visualize(communitiesHashtags);

    }

    private static void writeCsv4Visualize(MongoCursor<CommunitiesHashtag> communitiesHashtags) throws IOException {
        File file = new File("src/main/resources/temp.csv");
        file.createNewFile();

        PrintWriter pw = new PrintWriter(file);

        pw.write("Hashtag;Group;Polarity;Pol_Value\n");

        int i = 0;
        for (CommunitiesHashtag ch: communitiesHashtags){
            if (ch.getPolarity_value() > 75.0 || ch.getPolarity_value() < - 75.0) {
                for (String h : ch.getHashtags()) {
                    pw.write("" + h + ";" + i + ";" + ch.getPolarity() + ";" + Math.abs(ch.getPolarity_value()) + "\n");
                }
                i++;
            }
        }

        pw.close();
    }
}
