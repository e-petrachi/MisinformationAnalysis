package spark.analysis.post_analysis;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import spark.model.post_model.CommunitiesHashtag;
import spark.model.post_model.CommunitiesMention;
import spark.utilities.MongoRDDLoader;

import java.util.HashSet;
import java.util.TreeSet;

public class HMCommunities {

    private static final Logger LOG = Logger.getLogger(HMCommunities.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static Tuple3<JavaMongoRDD<Document>, JavaMongoRDD<Document>, JavaSparkContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader("bigdata","communitiesHashtag","bigdata","communities");

        JavaSparkContext jsc = ml.getSparkContext();

        JavaMongoRDD<Document> communitiesH = ml.getMongoRDD(jsc);
        JavaMongoRDD<Document> communitiesM = ml.getMongoRDDOverride(jsc, "bigdata", "communitiesMention");

        return new Tuple3<>(communitiesH,communitiesM,jsc);
    }

    public static void main(String[] args){
        Tuple3<JavaMongoRDD<Document>,JavaMongoRDD<Document>, JavaSparkContext> rdd_rdd2jsc = loadDocument();

        JavaSparkContext jsc = rdd_rdd2jsc._3();
        JavaRDD<CommunitiesHashtag> rdd_1 = rdd_rdd2jsc._1().map(doc -> new CommunitiesHashtag(doc));
        JavaRDD<CommunitiesMention> rdd_2 = rdd_rdd2jsc._2().map(doc -> new CommunitiesMention(doc));

        execute(rdd_1, rdd_2, jsc);

        jsc.close();

    }

    public static void execute(JavaRDD<CommunitiesHashtag> communitiesH, JavaRDD<CommunitiesMention> communitiesM, JavaSparkContext jsc) {

        // key + <hashtags_group, users_group, polarity, pol_values>
        JavaPairRDD<String, Tuple4<TreeSet<String>, HashSet<String>, String, Double>> h = communitiesH
                .mapToPair( a -> new Tuple2<>("g",new Tuple4<>(a.getHashtagsSet(),a.getUsersSet(),a.getPolarity(),a.getPolarity_value())));

        // key + <mentions_group, users_group, polarity, pol_values>
        JavaPairRDD<String,Tuple4<TreeSet<String>, HashSet<String>, String, Double>> m = communitiesM
                .mapToPair( a -> new Tuple2<>("g",new Tuple4<>(a.getMentionsSet(),a.getUsersSet(),a.getPolarity(),a.getPolarity_value())));

        // <hashtags_group, users_group, polarity, pol_values> + <mentions_group, users_group, polarity, pol_values> with same polarity with same users_groups
        JavaPairRDD<Tuple4<TreeSet<String>, HashSet<String>, String, Double>, Tuple4<TreeSet<String>, HashSet<String>, String, Double>> hm = h
                .join(m)
                .mapToPair(a -> a._2())
                .filter(a -> a._1()._3().equals(a._2()._3()))
                .filter( a -> compareTo(a._1()._2(),a._2()._2()));

        // <users_group,polarity> + [hashtags+mentions, ...]
        JavaPairRDD<Tuple2<HashSet<String>,String>, Iterable<TreeSet<String>> > c = hm
                .mapToPair( a -> (a._1()._2().size() > a._2()._2().size()) ?
                        new Tuple2<>(new Tuple2<>(a._2()._2(),a._1()._3()), combineTreeSet(a._1()._1(),a._2()._1())) :
                        new Tuple2<>(new Tuple2<>(a._1()._2(),a._1()._3()), combineTreeSet(a._1()._1(),a._2()._1()))  )
                .distinct()
                .groupByKey();

        // users_group_size + <users_group, hashtags+mentions, polarity>
        JavaPairRDD<Integer, Tuple3<HashSet<String>, TreeSet<String>, String>> communities = c
                .mapToPair( a -> {
                    TreeSet<String> set = new TreeSet<>();
                    a._2().forEach( b -> set.addAll(b));
                    return new Tuple2<>(a._1()._1().size(),new Tuple3<>(a._1()._1(),set,a._1()._2()));
                }).sortByKey(false)
                .distinct();


        JavaRDD<Document> mongordd = communities
                .map(a -> Document.parse("{'hashtags_mentions': " + a._2()._2() +
                        ", 'users': " + a._2()._1() +
                        ", 'size': " + a._1() +
                        ", 'polarity': '" + a._2()._3() +
                        "'}"));

        MongoSpark.save(mongordd, WriteConfig.create(jsc));

    }


    public static TreeSet<String> combineTreeSet(TreeSet<String> uno, TreeSet<String> due){
        uno.addAll(due);
        return uno;
    }

    public static boolean compareTo(HashSet<String> a ,HashSet<String> b){
        if (a.hashCode() != b.hashCode()){
            if (a.size() >= b.size()){
                return containsSome(a,b);
            } else {
                return containsSome(b,a);
            }
        }
        return true;
    }
    public static boolean containsSome(HashSet<String> max,HashSet<String> min){
        double threshold = 1;

        for (String s : min){
            if (!max.contains(s)){
                threshold = threshold - 1.0/min.size();
            }
            if (threshold < 0.8)
                return false;
        }
        return true;
    }
}
