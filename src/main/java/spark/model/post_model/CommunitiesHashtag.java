package spark.model.post_model;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;

public class CommunitiesHashtag implements Serializable {

    private ArrayList<String> hashtags;
    private ArrayList<String> users;
    private int size;
    private double polarity_value;
    private String polarity;

    public CommunitiesHashtag(){}

    public CommunitiesHashtag(Document doc) {
        JSONObject object = new JSONObject(doc.toJson());
        this.setSize(object.getInt("size"));
        this.setPolarity_value(object.getDouble("polarity_value"));
        this.setPolarity(object.getString("polarity"));
        this.setHashtags(object.optJSONArray("hashtags"));
        this.setUsers(object.optJSONArray("users"));
    }

    public ArrayList<String> getHashtags() {
        return hashtags;
    }

    public TreeSet<String> getHashtagsSet() {
        return new TreeSet<>(hashtags);
    }

    public void setHashtags(ArrayList<String> hashtags) {
        this.hashtags = hashtags;
    }

    public void setHashtags(JSONArray hashtags) {
        this.hashtags = new ArrayList<>();
        for (int i=0;i<hashtags.length();i++){
            this.hashtags.add("'#" + hashtags.getString(i) + "'");
        }
    }

    public ArrayList<String> getUsers() {
        return users;
    }

    public HashSet<String> getUsersSet() {
        return new HashSet<>(users);
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

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public double getPolarity_value() {
        return polarity_value;
    }

    public void setPolarity_value(double polarity_value) {
        this.polarity_value = polarity_value;
    }

    public String getPolarity() {
        return polarity;
    }

    public void setPolarity(String polarity) {
        this.polarity = polarity;
    }
}
