package spark.model.post_model;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;

public class MentionsGroup implements Serializable {

    private String mention;
    private int size;
    private ArrayList<String> users;

    private static final Logger LOG = Logger.getLogger(MentionsGroup.class);
    static { LOG.setLevel(Level.DEBUG);}

    public MentionsGroup(Document doc) {
        JSONObject object = new JSONObject(doc.toJson());
        this.setMention(object.getString("mention"));
        this.setSize(object.getInt("size"));
        this.setUsers(object.optJSONArray("users"));
    }

    public String getMention() { return mention; }

    public void setMention(String mention) { this.mention = mention; }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public ArrayList<String> getUsers() {
        return users;
    }

    public HashSet<String> getUsersSet() {
        return new HashSet<String>(users);
    }

    public void setUsers(ArrayList<String> users) {
        this.users = users;
    }

    public void setUsers(JSONArray users) {
        this.users = new ArrayList<>();
        for (int i=0;i<users.length();i++){
            this.users.add("'" + users.getString(i) + "'");
        }
    }
}
