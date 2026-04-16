import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class B1MovieRatingAnalysis {

    public static class RatingMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final IntWritable outKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 4) return;

            try {
                int movieId = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                outKey.set(movieId);
                outValue.set(rating + "\t1");
                context.write(outKey, outValue);
            } catch (NumberFormatException e) {
            }
        }
    }

    public static class RatingReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        private final Map<Integer, String> movieMap = new HashMap<>();
        private final DecimalFormat df = new DecimalFormat("0.00");

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            for (URI uri : cacheFiles) {
                String path = uri.getPath();
                if (path != null && path.endsWith("movies.txt")) {
                    loadMovies("movies.txt");
                }
            }
        }

        private void loadMovies(String fileName) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    int firstComma = line.indexOf(",");
                    int lastComma = line.lastIndexOf(",");

                    if (firstComma == -1 || lastComma == -1 || firstComma == lastComma) continue;

                    try {
                        int movieId = Integer.parseInt(line.substring(0, firstComma).trim());
                        String title = line.substring(firstComma + 1, lastComma).trim();
                        movieMap.put(movieId, title);
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            int count = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts.length != 2) continue;

                sum += Double.parseDouble(parts[0]);
                count += Integer.parseInt(parts[1]);
            }

            if (count == 0) return;

            double avg = sum / count;
            String title = movieMap.getOrDefault(key.get(), "UnknownMovie(" + key.get() + ")");
            String output = title + "\tAverageRating: " + df.format(avg) + " (TotalRatings: " + count + ")";
            context.write(new Text(output), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: hadoop jar ds200-java.jar B1MovieRatingAnalysis <ratings_1> <ratings_2> <output> <movies_file>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "B1 Movie Average Rating and Count");

        job.setJarByClass(B1MovieRatingAnalysis.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.addCacheFile(new URI(args[3] + "#movies.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
