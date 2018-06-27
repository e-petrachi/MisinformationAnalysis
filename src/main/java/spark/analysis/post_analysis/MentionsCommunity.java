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
import spark.model.post_model.GroupsMention;
import spark.model.post_model.Polarity;
import spark.utilities.MongoRDDLoader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

/**
 * Communities di utenti che hanno utilizzato le stesse mentions
 * (utili ad identificare comunità di diffusione automatica di misinformation)
 *
 */

public class MentionsCommunity {

    private static final Logger LOG = Logger.getLogger(MentionsCommunity.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static Tuple3<JavaMongoRDD<Document>, JavaMongoRDD<Document>, JavaSparkContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader("bigdata","mentionsgroup","bigdata","communitiesMention");

        JavaSparkContext jsc = ml.getSparkContext();

        JavaMongoRDD<Document> mentionsgroup = ml.getMongoRDD(jsc);
        JavaMongoRDD<Document> polarity = ml.getMongoRDDOverride(jsc, "bigdata", "polarity");

        return new Tuple3<>(mentionsgroup,polarity,jsc);
    }

    public static void main(String[] args){
        Tuple3<JavaMongoRDD<Document>,JavaMongoRDD<Document>, JavaSparkContext> rdd_rdd2jsc = loadDocument();

        JavaSparkContext jsc = rdd_rdd2jsc._3();
        JavaRDD<GroupsMention> rdd_1 = rdd_rdd2jsc._1().map(doc -> new GroupsMention(doc));
        JavaRDD<Polarity> rdd_2 = rdd_rdd2jsc._2().map(doc -> new Polarity(doc));

        execute(rdd_1, rdd_2, jsc);

        jsc.close();

    }

    public static void execute(JavaRDD<GroupsMention> mentionsgroup, JavaRDD<Polarity> polarity, JavaSparkContext jsc) {

        // key + <mention, users_group>
        JavaPairRDD<String,Tuple2<String, HashSet<String>>> t = mentionsgroup
                .mapToPair( a -> new Tuple2<>("mention",new Tuple2<>(a.getMention(),a.getUsersSet())))
                .filter( a -> a._2()._1().matches("[a-zA-Z0-9]*"));

        // key + <<mention, users_group>, <mention, users_group>>
        JavaPairRDD<String,Tuple2<Tuple2<String,HashSet<String>>,Tuple2<String,HashSet<String>>>> tt = t
                .join(t);

        // <<mention, users_group>, <mention, users_group>>
        JavaPairRDD<Tuple2<String,HashSet<String>>,Tuple2<String,HashSet<String>>> s = tt
                .mapToPair(a -> a._2())
                .filter( a -> !a._1()._1().equals(a._2()._1()));

        // <<mention, users_group>, <mention, users_group>>
        JavaPairRDD<Tuple2<String,HashSet<String>>,Tuple2<String,HashSet<String>>> s_star = s
                .filter( a -> HMCommunities.compareTo(a._1()._2(),a._2()._2()));

        // <<mention, users_group>, [mention, ...]>
        JavaPairRDD<Tuple2<String,HashSet<String>>, Iterable<String>> u = s_star
                .mapToPair( a -> (a._1()._2().size() > a._2()._2().size()) ?
                        new Tuple2<>(new Tuple2<>(a._2()._1(),a._2()._2()), a._1()._1()) : new Tuple2<>(new Tuple2<>(a._1()._1(),a._1()._2()), a._2()._1()))
                .distinct()
                .groupByKey();

        // size , <[mention, ...], users_group>
        JavaPairRDD<Integer, Tuple2<TreeSet<String>, HashSet<String>>> v = u
                .mapToPair( a -> {
                    TreeSet<String> set = new TreeSet<>();
                    set.add("'" + a._1()._1() + "'");
                    a._2().forEach( b -> set.add("'" + b + "'"));
                    return new Tuple2<>(a._1()._2().size(),new Tuple2<>(set, a._1()._2()));
                }).sortByKey(false)
                .distinct();

        // user, polarity
        JavaPairRDD<String,Integer> p = polarity.mapToPair( a -> new Tuple2<>(a.getUser(),a.getPolarity()));
        HashMap<String,Integer> user2polarity = new HashMap<>(p.collectAsMap());

        // size , <[mention, ...], users_group>
        JavaPairRDD<Tuple3<Integer,String,Double>, Tuple2<TreeSet<String>, HashSet<String>>> w = v
                .mapToPair( a -> {

                    ArrayList<Integer> values = new ArrayList<>();
                    double sum = 0;
                    a._2()._2().forEach( b -> values.add(user2polarity.containsKey(b) ? user2polarity.get(b) : 0));
                    for (Integer i : values){
                        sum += i;
                    }
                    String pol = sum > 0 ? "'information'" : "'misinformation'";

                    return new Tuple2<>(new Tuple3<>(a._1(), pol, Math.floor((sum/a._1())*10000.0)/100.0 ),a._2());
                });

        JavaRDD<Document> mongordd = w
                .map(a -> Document.parse("{'mentions': " + a._2()._1() +
                        ", 'users': " + a._2()._2() +
                        ", 'size': " + a._1()._1() +
                        ", 'polarity': " + a._1()._2() +
                        ", 'polarity_value': " + a._1()._3() +
                        "}"));

        MongoSpark.save(mongordd, WriteConfig.create(jsc));

    }
}
