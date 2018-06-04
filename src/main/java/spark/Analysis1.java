package spark;

import spark.temp.MongoRDDLoader;

public class Analysis1 {
    public static void main(String[] args){
        MongoRDDLoader ml = new MongoRDDLoader();
        ml.loader();
    }
}
