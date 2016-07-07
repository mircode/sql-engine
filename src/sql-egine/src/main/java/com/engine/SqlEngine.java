package com.engine;

import com.file.FileTable;
import com.file.Table;
import com.sql.SqlExeEngine;
import com.sql.SqlParse;

public class SqlEngine {

	// 操作表
	private Table table = null;
	// 操作SQL
	private SqlParse sqlParse = null;

	public SqlExeEngine SQL(String sql) {

		SqlExeEngine engine = new SqlExeEngine(table);

		this.sqlParse = new SqlParse(sql);

		// 获取到主表
		String from = sqlParse.get(SqlParse.FROM);
		if (from != null) {

			String name = SqlParse.getTable(from);
			String input = SqlParse.getPath(from);

			// 从input中解析出format和split
			this.table = new FileTable(name, input, null, null,null);
			engine.setTable(table);
		}

		// 执行join
		String join = sqlParse.get(SqlParse.JOIN);
		if (join != null) {

			for (String en : join.split("\\|")) {
				String table = en.split("on")[0];
				String on = en.split("on")[1];

				String name = SqlParse.getTable(table);
				String input = SqlParse.getPath(table);

				engine.join(new FileTable(name, input, null, null,null), on);
			}
		}

		// 执行where
		String where = sqlParse.get(SqlParse.WHERE);
		if (where != null) {
			engine.where(where);
		}

		// 执行group by
		String matrix = sqlParse.get(SqlParse.MATRIX);
		String group = sqlParse.get(SqlParse.GROUP);
		if (group != null || matrix != null) {
			engine.group(matrix, group);
		}

		// 执行过滤
		String select = sqlParse.get(SqlParse.SELECT);
		if (select != null) {
			engine.select(select);
		}
		String distinct = sqlParse.get(SqlParse.DISTINCT);
		if (distinct != null) {
			engine.distinct(distinct);
		}
		// 执行order by
		String order = sqlParse.get(SqlParse.ORDER);
		if (order != null) {
			engine.order(order);
		}
		// 执行limit
		String limit = sqlParse.get(SqlParse.LIMIT);
		if (limit != null) {
			engine.limit(limit);
		}

		return engine;
	}

	public static void main(String args[]) {

		// 声明变量
		String sql = null;
		SqlExeEngine sqlEngine = null;

		Table student = new FileTable("s","classpath:student.txt");
		Table teacher = new FileTable("t","classpath:teacher.txt");;

		// 简单查询
		sql = "select id,name,grade from classpath:student.txt student where id>10 order by grade desc limit 0,10";

		sqlEngine = new SqlEngine().SQL(sql);
		System.out.println(sqlEngine);

		sqlEngine = new SqlExeEngine(student).where("id>10").order("grade desc")
				.limit("0,10").select("id,name,grade");

		System.out.println(sqlEngine);

		// 查询最高的成绩
		sql = "select max(grade) as grade from classpath:student.txt s";

		sqlEngine = new SqlEngine().SQL(sql);
		System.out.println(sqlEngine);

		student = new FileTable("classpath:student.txt");
		String max = new SqlExeEngine(student).max("grade");
		System.out.println("grade");
		System.out.println(max);

		// 表连接
		sql = "select s.id,s.name,s.grade,t.id,t.name from classpath:student.txt s join classpath:teacher.txt t on s.tid=t.id limit 0,10";

		sqlEngine = new SqlEngine().SQL(sql);
		System.out.println(sqlEngine);

		teacher = new FileTable("t", "classpath:teacher.txt");
		student = new FileTable("s", "classpath:student.txt");

		sqlEngine = new SqlExeEngine(student).join(teacher, "s.tid=t.id")
				.select("s.id,s.name,s.grade,t.id,t.name").limit("0,10");
		System.out.println(sqlEngine);

		// 分组查询
		sql = "select tid,sum(grade) as grade from classpath:student.txt student group by tid limit 0,10";

		sqlEngine = new SqlEngine().SQL(sql);
		System.out.println(sqlEngine);

		student = new FileTable("s", "classpath:student.txt");
		sqlEngine = new SqlExeEngine(student).group("sum(grade) as grade", "tid")
				.limit("0,10");
		System.out.println(sqlEngine);

		// 表连接分组查询(查询那个老师对应的学生比较多)
		sql = "select t.name,count(t.id) as t.count  from classpath:student.txt s join classpath:teacher.txt t on s.tid=t.id group by t.id,t.name order by t.count desc limit 0,10";

		sqlEngine = new SqlEngine().SQL(sql);
		System.out.println(sqlEngine);

		teacher = new FileTable("t", "classpath:teacher.txt");
		student = new FileTable("s", "classpath:student.txt");

		sqlEngine = new SqlExeEngine(student).join(teacher, "s.tid=t.id")
				.group("count(t.id) as t.count", "t.id,t.name")
				.select("t.name,count(t.id) as t.count").order("t.count desc")
				.limit("0,10");
		System.out.println(sqlEngine);

	}
}
