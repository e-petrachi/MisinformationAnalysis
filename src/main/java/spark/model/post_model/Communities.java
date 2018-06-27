package spark.model.post_model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;

public class Communities implements Serializable {

    private ArrayList<String> hashtags_mentions;
    private ArrayList<String> users;
    private int size;
    private String polarity;

    public Communities(){}

    public ArrayList<String> getHashtags_mentions() {
        return hashtags_mentions;
    }

    public void setHashtags_mentions(ArrayList<String> hashtags_mentions) {
        this.hashtags_mentions = hashtags_mentions;
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

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getPolarity() {
        return polarity;
    }

    public void setPolarity(String polarity) {
        this.polarity = polarity;
    }
}
