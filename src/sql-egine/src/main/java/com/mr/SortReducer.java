package com.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.conf.SqlConf;
import com.file.Table;
import com.sql.SqlExeEngine;
import com.sql.SqlParse;

/**
 * 排序Reducer
 * 
 * @author 魏国兴
 */
public class SortReducer extends Reducer<Text, Text, NullWritable, Text> {

	// SQL解析器
	private SqlParse sqlParse = null;

	// 反序列化生成table
	private String serialize = null;

	public void setup(Context context) throws IOException, InterruptedException {
		// sql对象
		String sql = context.getConfiguration().get(SqlConf.CONF_SQL);
		sqlParse = new SqlParse(sql);

		// main表
		if (sqlParse.get(SqlParse.JOIN) != null) {
			serialize = context.getConfiguration()
					.get(SqlConf.CONF_JOINTABLE);
		} else {
			serialize = context.getConfiguration().get(sqlParse.get(SqlParse.MAIN_TABLE));
		}

		super.setup(context);
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// 初始化表
		Table table = initTable(values);

		// 构建SQL引擎
		SqlExeEngine sqlEngine = new SqlExeEngine(table);

		String distinct = sqlParse.get(SqlParse.DISTINCT);
		if (distinct != null) {
			sqlEngine.distinct(distinct);
		}
		// 执行order by
		String order = sqlParse.get(SqlParse.ORDER);
		if (order != null) {
			sqlEngine.order(order);
		}
		// 执行limit
		String limit = sqlParse.get(SqlParse.LIMIT);
		if (limit != null) {
			sqlEngine.limit(limit);
		}
		
		writeTable(context, sqlEngine.getTable());
	}

	private List<String> getRows(Iterable<Text> values) {
		List<String> rows = new ArrayList<String>();
		for (Text t : values) {
			rows.add(t.toString());
		}
		return rows;
	}

	/**
	 * 将Reduce中的values转化成Table
	 * 
	 * @param values
	 * @return
	 */
	private Table initTable(Iterable<Text> values) {

		// 反序列化生成table
		Table table = new Table().diserialize(serialize);
		// 分隔符
		String split = table.getSplit();
		table = new Table().diserialize(serialize);
		table.setFilter(null);
		String format = sqlParse.get("#mr.sort.format");
		if(!format.equals("*")){
			table.setFormat(format.replace(",", split));
		}
		table.setRows(getRows(values));

		return table;
	}

	/**
	 * 将表格的内容写入到HDFS中
	 * 
	 * @param context
	 * @param table
	 */
	private void writeTable(Context context, Table table) throws IOException,
			InterruptedException {
		for (String row : table.getRows()) {
			context.write(NullWritable.get(), new Text(row));
		}
	}
}
