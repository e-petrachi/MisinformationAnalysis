package spark.model.post_model;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;

public class Polarity implements Serializable {

    private String user;
    private int polarity;

    private static final Logger LOG = Logger.getLogger(Polarity.class);
    static { LOG.setLevel(Level.DEBUG);}

    public Polarity(Document doc) {
        JSONObject object = new JSONObject(doc.toJson());
        this.setUser("'" + object.getString("user") + "'");
        if (object.getDouble("misinformation") > object.getDouble("information"))
            this.polarity = -1;
        else
            this.polarity = 1;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public int getPolarity() {
        return polarity;
    }

}
