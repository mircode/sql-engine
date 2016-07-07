package com.conf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.file.Table;
import com.sql.SqlExeEngine;
import com.sql.SqlParse;

/**
 * 解析配置文件
 * 
 * @author 魏国兴
 *
 */
public class SqlConf extends Configuration {

	// 配置文件的属性
	public static final String CONF_SQL = "#sql";

	// 输入,输出,临时目录
	public static final String CONF_TMP = "#tmp";
	public static final String CONF_INPUT = "#input";
	public static final String CONF_OUTPUT = "#output";

	// 是否需要排序
	public static final String CONF_SORT = "#isSort";
	// 是否需要分组
	public static final String CONF_GROUP = "#isGroup";

	// Join临时表
	public static final String CONF_JOINTABLE = "#joinTable";

	// HDFS路径
	public static final String CONF_HDFS = "fs.defaultFS";

	// 解析SQL
	public SqlParse sqlParse = null;

	// SQL语句
	private String sql = null;
	// hdfs地址
	private String hdfs = null;

	// 输入目录
	private String input = null;
	// 输出目录
	private String output = null;
	// 临时目录
	private String tmp = null;

	// 是否需要执行排序任务
	private Boolean isSort = false;
	// 是否需要执行分组
	private Boolean isGroup = false;
	// Join之后表结构
	private String joinTable = null;
	// Table映射
	private Map<String, Table> tables = new HashMap<String, Table>();

	public SqlConf() {
	}

	public SqlConf(String hdfs, String sql, String output,
			Map<String, Table> tables) {

		// 解析输入输入和输出目录
		this.sqlParse = new SqlParse(sql);

		this.hdfs = hdfs;
		this.sql = sql;
		this.output = output;
		this.tables = tables;

		// 初始化临时目录
		initInput();

		// 计算Join之后的表结构
		initJoin();

		// 判断是否需要排序或分组
		initSort();
		initGroup();

		// 如果不需要排序和分组,那么临时目录就是输出目录
		if (!isSort || !isGroup) {
			this.tmp = this.output;
		}

		// 将变量加载到Context中
		loadContext();

	}

	/**
	 * 加载SQL和表映射到Context中
	 */
	public void loadContext() {

		// 加载SQL
		this.set(CONF_SQL, this.sql);

		// 执行SQL中的临时,输入,输出目录
		this.set(CONF_TMP, this.tmp);
		this.set(CONF_INPUT, this.input);
		this.set(CONF_OUTPUT, this.output);

		// 是否需要排序和分组
		this.setBoolean(CONF_GROUP, this.isGroup);
		this.setBoolean(CONF_SORT, this.isSort);

		// Join时的表结构
		if(joinTable!=null){
			this.set(CONF_JOINTABLE, this.joinTable);
		}

		// 加载HFDS地址
		this.set(CONF_HDFS, this.hdfs);

		// 加载其他表结构
		for (Map.Entry<String, Table> entry : this.tables.entrySet()) {
			String ser = entry.getValue().serialize();
			this.set(entry.getKey(), ser);
		}
	}

	// 是否需要排序
	private void initSort() {
		this.isSort = false;
		String distinct = sqlParse.get(SqlParse.DISTINCT);
		String order = sqlParse.get(SqlParse.ORDER);
		String limit = sqlParse.get(SqlParse.LIMIT);
		if (distinct != null || order != null || limit != null)
			this.isSort = true;
	}

	// 是否需要分组
	private void initGroup() {
		this.isGroup = false;
		String matrix = sqlParse.get(SqlParse.MATRIX);
		String group = sqlParse.get(SqlParse.GROUP);
		if (matrix != null || group != null)
			this.isGroup = true;
	}

	private void initJoin() {
		SqlExeEngine sqlEngine = new SqlExeEngine(tables.get(sqlParse
				.get(SqlParse.MAIN_TABLE)));
		String join = sqlParse.get(SqlParse.JOIN);
		// 如果有join的表,则Reduce端的表格式需要变更
		if (join != null) {
			for (String en : join.split("\\|")) {
				String table = en.split("on")[0];
				String on = en.split("on")[1];
				String name = SqlParse.getTable(table);
				sqlEngine.join(tables.get(name), on);
			}
			this.joinTable = sqlEngine.getTable().serialize();
		}
	}

	/**
	 * 初始化输入目录
	 */
	private void initInput() {
		this.input = this.tables.get(sqlParse.get(SqlParse.MAIN_TABLE)).getInput();
		if (output != null) {
			if (output.endsWith("/")) {
				output = output.substring(0, this.output.length() - 1);
			}
			this.tmp = output.substring(0, output.lastIndexOf("/") + 1)
					+ "tmp/";
		} else {
			if (input.endsWith("/")) {
				this.output = input + "out/";
				this.tmp = input + "tmp/";
			} else {
				this.output = input.substring(0, input.lastIndexOf("/") + 1)
						+ "out/";
				this.tmp = input.substring(0, input.lastIndexOf("/") + 1)
						+ "tmp/";
			}
		}
	}

}
