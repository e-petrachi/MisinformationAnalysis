package hadoop.analysis;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import hadoop.model.StringArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.bson.BSONObject;

public class Communities extends MongoTool {

    private static final Logger LOG = Logger.getLogger(Communities.class);

    public Communities() {
        long startTime = System.currentTimeMillis();

        JobConf conf = new JobConf(new Configuration());

        MongoConfigUtil.setInputFormat(conf, MongoInputFormat.class);
        MongoConfigUtil.setOutputFormat(conf, MongoOutputFormat.class);

        MongoConfig config = new MongoConfig(conf);

        config.setInputURI("mongodb://localhost:27017/bigdata.hashtagsgroup");

        config.setMapper(CommunitiesMapper.class);
        config.setReducer(CommunitiesReducer.class);

        config.setMapperOutputKey(Text.class);
        config.setMapperOutputValue(StringArrayWritable.class);

        config.setOutputKey(Text.class);
        config.setOutputValue(IntWritable.class);

        config.setOutputURI("mongodb://localhost:27017/bigdata.communities");
        setConf(conf);

        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    public static void main(final String[] pArgs) throws Exception {
        System.exit(ToolRunner.run(new Communities(), pArgs));
    }

}
