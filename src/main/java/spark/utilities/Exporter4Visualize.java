package spark.utilities;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jongo.MongoCursor;
import spark.model.post_model.Communities;
import spark.model.post_model.CommunitiesHashtag;
import spark.model.post_model.CommunitiesMention;

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
        MongoCursor<CommunitiesHashtag> communitiesHashtag = mongoLocal.findAllCommunitiesHashtag();
        writeCsv4VisualizeCommunitiesHashtag(communitiesHashtag, 99.0);
        writeCsv4VisualizeCommunitiesHashtagMin(communitiesHashtag);


        mongoLocal.setCollection("communitiesMention");
        MongoCursor<CommunitiesMention> communitiesMention = mongoLocal.findAllCommunitiesMention();
        writeCsv4VisualizeCommunitiesMention(communitiesMention, 99.0);
        writeCsv4VisualizeCommunitiesMentionMin(communitiesMention);


        mongoLocal.setCollection("communities");
        MongoCursor<Communities> communities = mongoLocal.findAllCommunities();
        writeCsv4VisualizeCommunities(communities);
    }

    private static void writeCsv4VisualizeCommunitiesHashtag(MongoCursor<CommunitiesHashtag> communities, double minPercentage) throws IOException {
        File file = new File("src/main/resources/communitiesHashtag" + (int) minPercentage + ".csv");
        file.createNewFile();

        PrintWriter pw = new PrintWriter(file);

        pw.write("Hashtag;Group;Size;Polarity;Pol_Value\n");

        int i = 0;
        for (CommunitiesHashtag ch: communities){
            if (ch.getPolarity_value() > minPercentage || ch.getPolarity_value() < minPercentage*(-1.0)) {
                for (String h : ch.getHashtags()) {
                    pw.write("" + h + ";" + i + ";" + ch.getSize() + ";" + ch.getPolarity() + ";" + Math.abs(ch.getPolarity_value()) + "\n");
                }
            }
            i++;
        }

        pw.close();
    }

    private static void writeCsv4VisualizeCommunitiesHashtagMin(MongoCursor<CommunitiesHashtag> communities) throws IOException {
        File file = new File("src/main/resources/communitiesHashtagMin.csv");
        file.createNewFile();

        PrintWriter pw = new PrintWriter(file);

        pw.write("Hashtag;Group;Size;Polarity;Pol_Value\n");

        int i = 0;
        for (CommunitiesHashtag ch: communities){
            if (ch.getPolarity_value() >= 100.0 || ch.getPolarity_value() <= -100.0) {
                if (ch.getSize() > 20) {
                    String finale = "";
                    for (String h : ch.getHashtags()) {
                        finale += "#" + h + " ";
                    }
                    pw.write("" + finale + ";" + i + ";" + ch.getSize() + ";" + ch.getPolarity() + ";" + Math.abs(ch.getPolarity_value()) + "\n");
                }
            }
            i++;
        }

        pw.close();
    }

    private static void writeCsv4VisualizeCommunitiesMention(MongoCursor<CommunitiesMention> communities, double minPercentage) throws IOException {
        File file = new File("src/main/resources/communitiesMention" + (int) minPercentage + ".csv");
        file.createNewFile();

        PrintWriter pw = new PrintWriter(file);

        pw.write("Mention;Group;Size;Polarity;Pol_Value\n");

        int i = 0;
        for (CommunitiesMention ch: communities){
            if (ch.getPolarity_value() > minPercentage || ch.getPolarity_value() < minPercentage*(-1.0)) {
                for (String m : ch.getMentions()) {
                    pw.write("" + m + ";" + i + ";" + ch.getSize() + ";" + ch.getPolarity() + ";" + Math.abs(ch.getPolarity_value()) + "\n");
                }
            }
            i++;
        }

        pw.close();
    }

    private static void writeCsv4VisualizeCommunitiesMentionMin(MongoCursor<CommunitiesMention> communities) throws IOException {
        File file = new File("src/main/resources/communitiesMentionMin.csv");
        file.createNewFile();

        PrintWriter pw = new PrintWriter(file);

        pw.write("Mention;Group;Size;Polarity;Pol_Value\n");

        int i = 0;
        for (CommunitiesMention ch: communities){
            if (ch.getPolarity_value() >= 100.0 || ch.getPolarity_value() <= -100.0) {
                if (ch.getSize() > 20) {
                    String finale = "";
                    for (String m : ch.getMentions()) {
                        finale += "@" + m + " ";
                    }
                    pw.write("" + finale + ";" + i + ";" + ch.getSize() + ";" + ch.getPolarity() + ";" + Math.abs(ch.getPolarity_value()) + "\n");
                }
            }
            i++;
        }

        pw.close();
    }

    private static void writeCsv4VisualizeCommunities(MongoCursor<Communities> communities) throws IOException {
        File file = new File("src/main/resources/communities.csv");
        file.createNewFile();

        PrintWriter pw = new PrintWriter(file);

        pw.write("HashtagOrMention;Group;Size;Polarity\n");

        int i = 0;
        for (Communities ch: communities){
            for (String m : ch.getHashtags_mentions()) {
                pw.write("" + m + ";" + i + ";" + ch.getSize() + ";" + ch.getPolarity() + "\n");
            }
            i++;
        }

        pw.close();
    }
}
