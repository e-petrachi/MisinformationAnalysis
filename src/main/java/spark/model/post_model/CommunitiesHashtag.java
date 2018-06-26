package spark.model.post_model;

import java.io.Serializable;
import java.util.ArrayList;

public class CommunitiesHashtag implements Serializable {

    private ArrayList<String> hashtags;
    private ArrayList<String> users;
    private int size;
    private double polarity_value;
    private String polarity;

    public ArrayList<String> getHashtags() {
        return hashtags;
    }

    public void setHashtags(ArrayList<String> hashtags) {
        this.hashtags = hashtags;
    }

    public ArrayList<String> getUsers() {
        return users;
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
