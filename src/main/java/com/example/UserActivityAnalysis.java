import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class UserActivityAnalysis {

    public static class SkipFirstLineInputFormat extends TextInputFormat {

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new SkipFirstLineRecordReader();  // 返回一个符合要求的 RecordReader
        }

        // 静态的 RecordReader 类，继承 LineRecordReader
        public static class SkipFirstLineRecordReader extends LineRecordReader {
            private boolean firstLineSkipped = false;

            @Override
            public boolean nextKeyValue() throws IOException {
                // 跳过第一行
                if (!firstLineSkipped) {
                    firstLineSkipped = true;
                    // 调用一次 nextKeyValue 跳过第一行
                    super.nextKeyValue();  
                    return super.nextKeyValue();  // 返回第二行及以后的内容
                } else {
                    return super.nextKeyValue();  // 正常处理
                }
            }
        }
    }

    public static class ActivityMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 9) return; 

            String userId = fields[0]; 

            double directPurchaseAmt = fields[5].isEmpty() ? 0 : Double.parseDouble(fields[5]); 
            double totalRedeemAmt = fields[8].isEmpty() ? 0 : Double.parseDouble(fields[8]);

            // 判断用户是否活跃
            if (directPurchaseAmt > 0 || totalRedeemAmt > 0) {
                context.write(new Text(userId), new LongWritable(1));  // 输出用户ID和活跃标记
            }
        }
    }

    public static class ActivityCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long activeDays = 0;
            for (LongWritable value : values) {
                activeDays += value.get();  // 统计活跃天数
            }
            context.write(key, new LongWritable(activeDays));
        }
    }

    public static class ActivityReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long activeDays = 0;
            for (LongWritable value : values) {
                activeDays += value.get();  // 统计活跃天数
            }
            context.write(key, new LongWritable(activeDays));
        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 2) return;  // 确保字段足够

            String userId = fields[0];  // 用户ID
            long activeDays = Long.parseLong(fields[1]);  // 活跃天数

            context.write(new LongWritable(activeDays), new Text(userId));  // 输出活跃天数和用户ID
        }
    }

    public static class SortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, key);  // 输出用户ID和活跃天数
            }
        }
    }

    public static class DescendingComparator extends WritableComparator {
        protected DescendingComparator() {
            super(LongWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            LongWritable val1 = (LongWritable) w1;
            LongWritable val2 = (LongWritable) w2;
            return val2.compareTo(val1);  // 降序排列
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "User Activity Analysis");

        job1.setJarByClass(UserActivityAnalysis.class);
        job1.setMapperClass(ActivityMapper.class);
        job1.setCombinerClass(ActivityCombiner.class);
        job1.setReducerClass(ActivityReducer.class);
        job1.setInputFormatClass(SkipFirstLineInputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/tmp"));

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "User Activity Sort");

        job2.setJarByClass(UserActivityAnalysis.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);

        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setSortComparatorClass(DescendingComparator.class);  

        FileInputFormat.addInputPath(job2, new Path(args[1] + "/tmp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/result"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
