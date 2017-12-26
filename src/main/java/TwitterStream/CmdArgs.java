package TwitterStream;

import org.kohsuke.args4j.Option;

/**
 * Created by user on 2017/3/8.
 */
public class CmdArgs {
    @Option(name="-numTweets",usage="Set sum Of Tweets")
    public static int numTweets = 100;

    @Option(name="-numInSleepTime",usage="Set search Sleep Time")
    public static int numInSleepTime = 100;

    @Option(name="-numOutSleepTime",usage="Set iteration Sleep Time")
    public static int numOutSleepTime = 300;

    @Option(name="-outputPath",usage="Set output path")
    public static String outputPath;
}
