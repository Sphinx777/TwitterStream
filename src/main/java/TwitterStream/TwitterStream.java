package TwitterStream; /**
 * Created by user on 2017/3/1.
 */
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.kohsuke.args4j.CmdLineParser;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class TwitterStream {
    //count4Topic
    //sumOfTweets
    public static void main(String[] args) throws Exception {
        //column format: 0, 1, 2=date, 3=topic, 4=user, 5=tweet
        final String[] searchTermList = {"@apple","#google","#microsoft","#twitter"};
        int tmpTweetSum =0;
        String tmpStr;
        List<String> tweetList = new ArrayList<String>();

        CmdArgs cmdArgs = new CmdArgs();
        CmdLineParser parser = new CmdLineParser(cmdArgs);
        parser.parseArgument(args);
        String paraFilePath = CmdArgs.outputPath + File.separator + "rawData_" + new SimpleDateFormat("yyyy-MMdd-HHmmss", Locale.ENGLISH).format(new Date())+"_"+CmdArgs.numTweets +".csv";

        //initialize spark session

        //for local build
        System.setProperty("hadoop.home.dir", "D:\\JetBrains\\IntelliJ IDEA Community Edition 2016.2.4");
        System.setProperty("spark.hadoop.dfs.replication", "1");

        SparkSession sparkSession = SparkSession.builder()
                //for local build
                .master("local")
                .appName("TwitterStream")
                .config("spark.sql.warehouse.dir", "file:///")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        System.out.println("Initialize OK!");
        do {
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true);
//            cb.setOAuthConsumerKey("MyOAuthConsumerKey");
//            cb.setOAuthConsumerSecret("MyOAuthConsumerSecret");
//            cb.setOAuthAccessToken("MyOAuthAccessToken");
//            cb.setOAuthAccessTokenSecret("MyOAuthAccessTokenSecret");

            Twitter twitter = new TwitterFactory(cb.build()).getInstance();
            Query query;
            QueryResult result;

            for (String searchTerm:searchTermList) {
                query = new Query(searchTerm);
                query.setLang("en");
                result = twitter.search(query);
                if(result.nextQuery() != null){
                    List<Status> tweets = result.getTweets();
                    System.out.println("search Term:"+searchTerm+"--"+tweets.size());
                    tmpTweetSum += tweets.size();
                    System.out.println("tmpSum:"+tmpTweetSum);
                    for (Status tweet : tweets) {
                        if(StringUtils.isNotEmpty(tweet.getUser().getScreenName()) && StringUtils.isNotEmpty(tweet.getText()) && tweet.getCreatedAt()!=null) {
                            System.out.println("0,1," + tweet.getCreatedAt() + "," + searchTerm + "," + tweet.getUser().getScreenName() + "," + tweet.getText());
                            tmpStr = "0,1," + tweet.getCreatedAt() + "," + searchTerm + "," + tweet.getUser().getScreenName() + "," + tweet.getText().replace("\n", "").replace("\r", "") + System.getProperty("line.separator");
                            tweetList.add(tmpStr);
                        }
                    }
                }
                Thread.sleep(CmdArgs.numOutSleepTime);
            }
            Thread.sleep(CmdArgs.numInSleepTime);
        }while (tmpTweetSum < CmdArgs.numTweets);
        writeToExcel(tweetList, CmdArgs.outputPath ,paraFilePath, sc.hadoopConfiguration());
        System.out.println("ok,total output:"+tmpTweetSum);
    }

    public static void writeToExcel(List<String> tweetList,String outFilePath , String paraFilePath, Configuration conf){
        FileSystem hdfsFS; //for hdfs
        BufferedWriter br;
        try {
            System.out.println("the outFilepath:"+paraFilePath);
            if(outFilePath.startsWith("hdfs://")) {
                System.out.println("fs.default.name:" + conf.get("fs.defaultFS"));
                hdfsFS = FileSystem.get(conf);
                Path folderPath = new Path(outFilePath);

                //check result file
                if (hdfsFS.exists(folderPath))
                //{
                //    System.out.println("the outFile exist , so delete!");
                //     hdfsFS.delete(folderPath, true);
                //} else
                {
                    System.out.println("the outFile is not exist , so create!");
                    hdfsFS.mkdirs(folderPath);
                }

                //check parameter file
                Path hdfsFilePath = new Path(paraFilePath);
//                if (hdfsFS.exists(hdfsFilePath)) {
//                    hdfsFS.delete(hdfsFilePath, true);
//                }

                OutputStream os = hdfsFS.create(hdfsFilePath,
                        new Progressable() {
                            @Override
                            public void progress() {
                                System.out.println("...bytes written...");
                            }
                        }
                );

                br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
                for (String tweet:tweetList) {
                    br.write(tweet);
                }
                br.close();
                //hdfsFS.close();
            }else{
                outFilePath = outFilePath.replace("file://","");
                System.out.println("OK , create local file:"+outFilePath);
                File folder = new File(outFilePath);

                //check result folder
                if (!folder.exists())
                //{
                //    System.out.println("the outFile exist , so delete!");
                //    folder.delete();
                //} else
                {
                    System.out.println("the outFile is not exist , so create!");
                    folder.mkdirs();
                }

                paraFilePath = paraFilePath.replace("file://","");
                System.out.println("the parameter path:"+paraFilePath);
                //check parameter file
                File paraFile = new File(paraFilePath);
//                if (paraFile.exists()) {
//                    paraFile.delete();
//                }
                FileWriter fw = new FileWriter(paraFilePath, true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter writer = new PrintWriter(bw);
                //PrintWriter writer = new PrintWriter(paraFilePath, "UTF-8");
                for (String tweet:tweetList) {
                    writer.write(tweet);
                }

                writer.close();
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}