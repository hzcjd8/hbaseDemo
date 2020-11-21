package lession2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import util.HBaseDAOImp;

public class WCRunner {

	public static void main(String[] args) throws Exception {
		
		//建表
		HBaseDAOImp dao=new HBaseDAOImp();
	    String tableName="wc";
		String cfs[] = {"cf"};
		dao.deleteTable(tableName);
		dao.createTable(tableName, cfs);
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://ha:9000");
		conf.set("hbase.zookeeper.quorum", "hb");
		Job job = Job.getInstance(conf);
		job.setJarByClass(WCRunner.class);

		// 指定mapper 和 reducer
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 最后一个参数设置false
		// TableMapReduceUtil.initTableReducerJob(table, reducer, job);
		TableMapReduceUtil.initTableReducerJob("wc", WCReducer.class, job, null, null, null, null, false);
		FileInputFormat.addInputPath(job, new Path("/data/hdfs_wc/input/"));
		job.waitForCompletion(true);                
		
		
		dao.scaner(tableName);//查询表中所有数据
	}
}
