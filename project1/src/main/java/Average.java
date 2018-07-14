import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by saleena on 14/7/18.
 */
public class Average {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text gender = new Text();
        private IntWritable age = new IntWritable();
        public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
            String line = value.toString();
            String str[]=line.split(",");
            if(str.length>6){
                gender.set(str[4]);
                if((str[1].equals("0")) ){
                    if(str[5].matches("\\d+")){
                        int i=Integer.parseInt(str[5]);
                        age.set(i);

                    }
                }
            }
            context.write(gender, age);

        }

    }

    public static class Reduce extends Reducer<Text,IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int l=0;
            for (IntWritable val : values) {
                l+=1;
                sum += val.get();
            }
            sum=sum/l;
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Averageage_survived");
        job.setJarByClass(Average.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //  job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out=new Path(args[1]);
        out.getFileSystem(conf).delete(out);
        job.waitForCompletion(true);
    }


}
