import com.kokutouda.wordcount.WordCountDemo;
import org.apache.hadoop.util.ProgramDriver;

public class DemoDriver {
    public static void main(String args[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();
        try {
            programDriver.addClass("wordcount", WordCountDemo.class, "computing word count");
            exitCode = programDriver.run(args);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        System.exit(exitCode);
    }
}
