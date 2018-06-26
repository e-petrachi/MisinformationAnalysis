package spark.utilities;


import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Level;
import org.jongo.*;
import spark.model.post_model.CommunitiesHashtag;

public class MongoLocal {
    private String dbName;
    private MongoClient mongo;
    private MongoDatabase database;
    private DB db;
    private Jongo jongo;

    private MongoCollection collection;

    // timeout a 15/30/45 minuti
    private static final int CONNECTION_TIME_OUT_MS = 900000;        // default is 10000
    private static final int SOCKET_TIME_OUT_MS = 1800000;           // default is 20000
    private static final int SERVER_SELECTION_TIMEOUT_MS = 2700000;  // default is 30000

    // logger usato da me
    private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(MongoLocal.class);
    static { LOG.setLevel(Level.INFO);}

    public MongoLocal(){

        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();
        optionsBuilder.connectTimeout(CONNECTION_TIME_OUT_MS);
        optionsBuilder.socketTimeout(SOCKET_TIME_OUT_MS);
        optionsBuilder.serverSelectionTimeout(SERVER_SELECTION_TIMEOUT_MS);

        final MongoClientOptions options = optionsBuilder.build();

        this.mongo = new MongoClient("localhost:27017", options);

        LOG.trace(this.mongo.toString());
    }

    public void setDbName(String dbName){
        this.dbName = dbName;
        this.database = mongo.getDatabase(this.dbName);
        this.db = mongo.getDB(this.dbName);
        this.jongo = new Jongo(db);
    }

    public DB getDb() { return this.db; }
    public void setCollection(String name){
        LOG.debug("setting mongo collection at [" + name + "]");
        this.collection = jongo.getCollection(name);
    }
    public String getCollectionName(){ return this.collection.getName(); }
    public MongoCollection getCollection() { return this.collection; }

    public void close() {
        LOG.debug("closing mongo connection");
        this.mongo.close();
    }

    public MongoCursor<CommunitiesHashtag> findAllCommunitiesHashtag() {
        return collection.find().as(CommunitiesHashtag.class);
    }

}
