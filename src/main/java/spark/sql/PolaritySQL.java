package spark.sql;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.bson.Document;
import scala.Tuple2;
import scala.Tuple3;
import spark.model.Constant;
import spark.model.QueryResult;
import spark.utilities.MongoRDDLoader;
import org.apache.spark.sql.functions.*;

import java.util.ArrayList;

/**
 * Polarità degli utenti con le relative percentuali e quantità di tweet per ognuno di essi +
 * Identificazione di utenti fonte di misinformation o information in funzione del rapporto friends/followers
 * (valori bassi rispetto alla media indicano probabili fonti < soglia 1.0)
 */

public class PolaritySQL {

    private static final Logger LOG = Logger.getLogger(PolaritySQL.class);
    static { LOG.setLevel(Level.DEBUG);}

    public static Tuple2<Dataset<Row>, SQLContext> loadDocument() {
        MongoRDDLoader ml = new MongoRDDLoader("bigdata","polarity","","");
        return ml.openloaderSQL();
    }

    public static void main(String[] args){

        Tuple2<Dataset<Row>, SQLContext> rdd2jsc = loadDocument();

        execute(rdd2jsc._1(), rdd2jsc._2());

        rdd2jsc._2().sparkSession().close();
    }

    public static void execute(Dataset<Row> dataset, SQLContext sc) {

        Dataset<Row> ds_1 = dataset
                .select("user","misinformation","information", "friends_followers", "tweets")
                .where("misinformation > 75 OR information > 75")
                .where("tweets > 5")
                .where("friends_followers < 1")
                .orderBy(org.apache.spark.sql.functions.desc("tweets"));

        ds_1.show();


        Dataset<Row> ds1 = ds_1
                .select("misinformation","information")
                .groupBy()
                .count();

        Dataset<Row> mis1 = ds_1
                .select("misinformation" )
                .where("misinformation > 75")
                .groupBy()
                .count();

        Dataset<Row> inf1 = ds_1
                .select("information")
                .where("information > 75")
                .groupBy()
                .count();

        long all = ds1.first().getLong(0);
        long misinformation = mis1.first().getLong(0);
        long information = inf1.first().getLong(0);

        LOG.info("UTENTI CON friends_followers < 1 TROVATI:\n \tTOTALI: " + all + "\tMISINFORMATION: " + misinformation + "\tINFORMATION: " + information);



        Dataset<Row> ds_2 = dataset
                .select("user","misinformation","information", "friends_followers", "tweets")
                .where("misinformation > 75 OR information > 75")
                .where("tweets > 5")
                .where("friends_followers < 0.01")
                .orderBy(org.apache.spark.sql.functions.desc("tweets"));

        ds_2.show();

        Dataset<Row> ds2 = ds_2
                .select("misinformation","information")
                .groupBy()
                .count();

        Dataset<Row> mis2 = ds_2
                .select("misinformation" )
                .where("misinformation > 75")
                .groupBy()
                .count();

        Dataset<Row> inf2 = ds_2
                .select("information")
                .where("information > 75")
                .groupBy()
                .count();

        all = ds2.first().getLong(0);
        misinformation = mis2.first().getLong(0);
        information = inf2.first().getLong(0);

        LOG.info("UTENTI CON friends_followers < 0.01 TROVATI:\n \tTOTALI: " + all + "\tMISINFORMATION: " + misinformation + "\tINFORMATION: " + information);


        Dataset<Row> ds_3 = dataset
                .select("user","misinformation","information", "friends_followers", "tweets")
                .where("misinformation > 75 OR information > 75")
                .where("tweets > 5")
                .orderBy(org.apache.spark.sql.functions.desc("tweets"));

        ds_3.show();

        Dataset<Row> ds3 = ds_3
                .select("misinformation","information")
                .groupBy()
                .count();

        Dataset<Row> mis = ds_3
                .select("misinformation" )
                .where("misinformation > 75")
                .groupBy()
                .count();

        Dataset<Row> inf = ds_3
                .select("information")
                .where("information > 75")
                .groupBy()
                .count();

        all = ds3.first().getLong(0);
        misinformation = mis.first().getLong(0);
        information = inf.first().getLong(0);

        LOG.info("UTENTI SENZA RESTRINZIONI TROVATI:\n \tTOTALI: " + all + "\tMISINFORMATION: " + misinformation + "\tINFORMATION: " + information);
    }
}
