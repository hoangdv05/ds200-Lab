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

public class B3GenderRatingAnalysis {

    public static class GenderMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final Map<Integer, String> userGender = new HashMap<>();
        private final IntWritable outKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            for (URI uri : cacheFiles) {
                String path = uri.getPath();
                if (path != null && path.endsWith("users.txt")) {
                    loadUsers("users.txt");
                }
            }
        }

        private void loadUsers(String fileName) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    String[] parts = line.split(",");
                    if (parts.length < 2) continue;

                    try {
                        int userId = Integer.parseInt(parts[0].trim());
                        String gender = parts[1].trim();
                        userGender.put(userId, gender);
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 4) return;

            try {
                int userId = Integer.parseInt(parts[0].trim());
                int movieId = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                String gender = userGender.get(userId);
                if (gender == null) return;

                outKey.set(movieId);
                outValue.set(gender + "\t" + rating);
                context.write(outKey, outValue);
            } catch (NumberFormatException e) {
            }
        }
    }

    public static class GenderReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
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

            double maleSum = 0.0, femaleSum = 0.0;
            int maleCount = 0, femaleCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts.length != 2) continue;

                String gender = parts[0].trim();
                double rating = Double.parseDouble(parts[1].trim());

                if ("M".equalsIgnoreCase(gender)) {
                    maleSum += rating;
                    maleCount++;
                } else if ("F".equalsIgnoreCase(gender)) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }

            String title = movieMap.getOrDefault(key.get(), "UnknownMovie(" + key.get() + ")");
            String maleAvg = maleCount == 0 ? "N/A" : df.format(maleSum / maleCount);
            String femaleAvg = femaleCount == 0 ? "N/A" : df.format(femaleSum / femaleCount);

            String output = title + "\tMale_Avg: " + maleAvg + ", Female_Avg: " + femaleAvg;
            context.write(new Text(output), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: hadoop jar ds200-java.jar B3GenderRatingAnalysis <ratings_1> <ratings_2> <output> <users_file> <movies_file>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "B3 Gender Rating Analysis");

        job.setJarByClass(B3GenderRatingAnalysis.class);
        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.addCacheFile(new URI(args[3] + "#users.txt"));
        job.addCacheFile(new URI(args[4] + "#movies.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
