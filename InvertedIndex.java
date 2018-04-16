import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InvertedIndex {

    public static class InvertedIndexMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        private final static Text word = new Text();
        private final static Text DocID = new Text();

        public void map(LongWritable key, Text val,
                        OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            DocID.set(fileName);

            String line = val.toString();
            StringTokenizer itr = new StringTokenizer(line.toLowerCase());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                output.collect(word, DocID);
            }
        }
    }


    public static class InvertedIndexReducer extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

//            System.out.println(key);
//            while (values.hasNext()){
//                System.out.println(values.next());
//            }

            HashSet<String> index=new HashSet<String>();
            String[] arr = {"little","jewel","believe","jovian","harriet","large","first","love"};
            for (String x: arr) {
                index.add(x);
//
            }
//            System.out.println(index);


            Map<String, Integer> Word_DocID = new HashMap<String, Integer>();
            StringBuilder toReturn = new StringBuilder();

            while (values.hasNext()) {

                String DocID = values.next().toString();

                if (Word_DocID.containsKey(DocID)) {
                    Integer DocIDCount = Word_DocID.get(DocID);

                    Word_DocID.put(DocID, DocIDCount + 1);
                } else {
                    Word_DocID.put(DocID, 1);
                }

            }
                for (Map.Entry<String, Integer> entry : Word_DocID.entrySet()) {
                    toReturn.append(entry.getKey()).append(":").append(entry.getValue()+" ");
//                    System.out.println(toReturn);

                }

//                if(index.contains(key)){
//                output.collect(key,new Text(toReturn.toString()));
//                }
            output.collect(key, new Text(toReturn.toString()));
        }
    }


    /**
     * The actual main() method for our program; this is the
     * "driver" for the MapReduce job.
     */
    public static void main(String[] args) {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(InvertedIndex.class);

        conf.setJobName("LineIndexer");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);


        FileSystem fs = null;
        Path outpath = new Path(args[1]);
        try {
            fs = outpath.getFileSystem(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            if (fs.exists(outpath)) {
       /*If exist delete the output path*/
                fs.delete(outpath, true);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        FileInputFormat.addInputPath(conf, new Path(args[0]));
                FileOutputFormat.setOutputPath(conf, new Path(args[1]));

                conf.setMapperClass(InvertedIndexMapper.class);
                conf.setReducerClass(InvertedIndexReducer.class);

                client.setConf(conf);


                try {
                    JobClient.runJob(conf).waitForCompletion();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

    }


