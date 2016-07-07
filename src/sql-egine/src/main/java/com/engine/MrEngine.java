package com.engine;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.conf.SqlChain;
import com.conf.SqlConf;
import com.hdfs.HDFSHelper;
import com.mr.SortMapper;
import com.mr.SortReducer;
import com.mr.SqlCombiner;
import com.mr.SqlMapper;
import com.mr.SqlReducer;

/**
 * 通过Hadoop执行SQL任务
 * @author 魏国兴
 *
 */
public class MrEngine {

	public static void main(String[] args) throws Exception {
		// 配置文件路径
		String path = "classpath:chain.conf";
		if(args.length>1){
			path=args[1];
		}
		// SQL引擎
		MrEngine engine = new MrEngine();
		// 执行SQL
		engine.execute(path);
	}

	// 执行SQL
	public void execute(String path) throws IOException,
			ClassNotFoundException, InterruptedException {

		// 执行多条SQL
		List<SqlConf> confs = SqlChain.getChain(path);

		// 链式执行SQL
		for (SqlConf conf : confs) {
			execute(conf);
		}
		// 打印执行结果
		for (SqlConf conf : confs) {
			System.out.println(conf.get(SqlConf.CONF_SQL));
			// 打印执行结果
			System.out.println(conf.sqlParse.get("#mr.sort.format").replace(",", "|"));
			List<String> result = HDFSHelper.readLines(conf, conf.get("#output")
					+ "/part-r-00000",20);
			for(String row:result)
				System.out.println(row);
		}
	}

	public void execute(SqlConf conf) throws IOException,
			ClassNotFoundException, InterruptedException {

		// #########################################
		// Job 控制器
		// #########################################

		// Job控制器
		JobControl jobControl = new JobControl(MrEngine.class.getName()
				+ "_JobChain");

		
		if (conf.getBoolean(SqlConf.CONF_GROUP, false)) {
			Job mainJob = this.groupJob(conf);
			// 主Job
			ControlledJob cmainJob = new ControlledJob(
					mainJob.getConfiguration());
			cmainJob.setJob(mainJob);
			jobControl.addJob(cmainJob);

			Job sortJob = this.sortJob(conf);
			if (conf.getBoolean(SqlConf.CONF_SORT, false)) {
				// 排序Job
				ControlledJob csortJob = new ControlledJob(
						sortJob.getConfiguration());
				csortJob.setJob(sortJob);
				csortJob.addDependingJob(cmainJob);
				jobControl.addJob(csortJob);
			}
		} else {
			Job filterJob = this.filterJob(conf);
			// 主Job
			ControlledJob cfilterJob = new ControlledJob(
					filterJob.getConfiguration());
			cfilterJob.setJob(filterJob);
			jobControl.addJob(cfilterJob);
		}
		// 运行Job
		new Thread(jobControl).start();

		// 等待执行完成,并打印执行进度
		while (!jobControl.allFinished()) {
			Thread.sleep(500);
			for (ControlledJob job : jobControl.getRunningJobList()) {
				this.display(job.getJob());
			}
		}
		jobControl.stop();
	}

	public void display(Job job) {
		try {
			// JobConf conf=(JobConf)job.getConfiguration();
			System.out.printf("Job " + job.getJobName()
					+ ": map: %.1f%% reduce %.1f%%\n",
					100.0 * job.mapProgress(), 100.0 * job.reduceProgress());
			// System.out.println("Job total maps = " + conf.getNumMapTasks());
			// System.out.println("Job total reduces = " +
			// conf.getNumReduceTasks());
			// System.out.flush();
		} catch (Exception e) {
		}

	}

	public Job filterJob(Configuration conf) throws IOException {
		// #########################################
		// 分组Job
		// #########################################

		// 清除输出目录
		HDFSHelper.deleteOnExit(conf, conf.get(SqlConf.CONF_OUTPUT));

		// 设置输入
		Path in = new Path(conf.get(SqlConf.CONF_INPUT));
		Path out = new Path(conf.get(SqlConf.CONF_OUTPUT));

		Job job = Job.getInstance(conf, MrEngine.class.getName() + "_filter");
		job.setJarByClass(MrEngine.class);
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setMapperClass(SqlMapper.class);
		job.setReducerClass(SortReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// System.exit(mainJob.waitForCompletion(true)?0:1);

		return job;
	}

	public Job groupJob(Configuration conf) throws IOException {
		// #########################################
		// 分组Job
		// #########################################

		// 清除输出目录
		HDFSHelper.deleteOnExit(conf, conf.get(SqlConf.CONF_TMP));

		// 设置输入
		Path in = new Path(conf.get(SqlConf.CONF_INPUT));
		Path out = new Path(conf.get(SqlConf.CONF_TMP));

		Job job = Job.getInstance(conf, MrEngine.class.getName() + "_main");
		job.setJarByClass(MrEngine.class);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setMapperClass(SqlMapper.class);
		job.setReducerClass(SqlReducer.class);
		job.setCombinerClass(SqlCombiner.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// System.exit(mainJob.waitForCompletion(true)?0:1);

		return job;
	}

	public Job sortJob(Configuration conf) throws IOException {

		// #########################################
		// 排序 Job
		// #########################################

		// 清除输出目录
		HDFSHelper.deleteOnExit(conf, conf.get(SqlConf.CONF_OUTPUT));

		// 设置输入
		Path sortIn = new Path(conf.get(SqlConf.CONF_TMP));
		Path sortOut = new Path(conf.get(SqlConf.CONF_OUTPUT));

		Job job = Job.getInstance(conf, MrEngine.class.getName() + "_sort");
		job.setJarByClass(MrEngine.class);
		FileInputFormat.setInputPaths(job, sortIn);
		FileOutputFormat.setOutputPath(job, sortOut);

		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		return job;
	}

}
