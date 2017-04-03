/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.samples.hadoop.mapreduce;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class HashtagCount extends Configured implements Tool{

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, LongWritable> {

		final static Pattern TAG_PATTERN = Pattern.compile("\"hashTags\":\\[([^\\]]*)");
		private final static LongWritable ONE = new LongWritable(1L);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Matcher matcher = TAG_PATTERN.matcher(value.toString());
			while (matcher.find()) {
				String found = matcher.group();
				String cleanedString = found.replaceFirst("\"hashTags\":\\[\\{\"text\":\"", "");
				String superPolished = cleanedString.split("\",\"")[0];

				String useMe = superPolished;
				if (superPolished.startsWith("\\u")) {
					useMe = StringEscapeUtils.unescapeJava(superPolished);
				}
				useMe = useMe.split("\"")[0];

				word.set(useMe.toLowerCase());
				context.write(word, ONE);
			}
		}

	}

	public static class LongSumReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		// framework is now "yarn", should be defined like this in mapred-site.xm
//		conf.set("yarn.resourcemanager.address", "hdp24:8050"); 
//		conf.get("mapreduce.framework.name", "yarn");
//		conf.setInt("mapreduce.map.memory.mb", 256);
//		conf.set("mapreduce.map.javaopts", "-Xmx256m");
//		conf.setInt("mapreduce.map.cpu.vcores", 1);
//		conf.setInt("mapreduce.reduce.memory.mb", 256);
//		conf.set("mapreduce.reduce.javaopts", "-Xmx256m");
//		conf.setInt("mapreduce.reduce.cpu.vcores", 1);
//		conf.set("hdp.version", "current");
//		conf.set("fs.default.name", "hdfs://hdp24:8020");
//		conf.set("yarn.application.classpath", "/etc/hadoop/conf,/usr/hdp/current/hadoop-client/*,/usr/hdp/current/hadoop-client/lib/*,/usr/hdp/current/hadoop-hdfs-client/*,/usr/hdp/current/hadoop-hdfs-client/lib/*,/usr/hdp/current/hadoop-yarn-client/*,/usr/hdp/current/hadoop-yarn-client/lib/*,/usr/hdp/current/hadoop-mapreduce-client/*,/usr/hdp/current/hadoop-mapreduce-client/lib/*");
//		conf.set("mapreduce.application.classpath", "/usr/hdp/current/hadoop-mapreduce-client/*:/usr/hdp/current/hadoop-mapreduce-client/lib/*:/usr/hdp/current/hadoop-client/*:/usr/hdp/current/hadoop-client/lib/*:/usr/hdp/current/hadoop-yarn-client/*:/usr/hdp/current/hadoop-yarn-client/lib/*:/usr/hdp/current/hadoop-hdfs-client/*:/usr/hdp/current/hadoop-hdfs-client/lib/*:/etc/hadoop/conf/secure:/mr-framework/*");
		

		System.out.println(String.format("fs.defaultFS: %s", conf.get("fs.defaultFS")));
		System.out.println(String.format("mapreduce.framework.name: %s", conf.get("mapreduce.framework.name")));
		System.out.println(String.format("yarn.resourcemanager.address: %s", conf.get("yarn.resourcemanager.address")));
        for (Map.Entry<String, String> entry : conf) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
//		String[] otherArgs = new GenericOptionsParser(conf, args)
//				.getRemainingArgs();
//		if (otherArgs.length != 2) {
//			System.err.println("Usage: HaashtagCount <in> <out>");
//			System.exit(2);
//		}
		Job job = new Job(conf, "hashtag count");
		job.setJar("/media/sf_vmshare/src/batch-mapreduce/mr-batch/target/batch-mapreduce-1.0.0.BUILD-SNAPSHOT.jar");
//		job.setJarByClass(HashtagCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path("/tweets/input"));
		FileOutputFormat.setOutputPath(job, new Path("/tweets/output"));
//		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
