package com.sql;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL解析器
 * 
 * @author 魏国兴
 * 
 */
public class SqlParse {

	// 输入输出路径
	public static final String INPUT="sql.input";
	public static final String OUTPUT="sql.output";
	
	// 主表和Join表
	public static final String MAIN_TABLE="#main.table";
	public static final String JOIN_TABLE="#join.table";
	
	// SQL元语
	public static final String CREATE="create";
	public static final String SELECT="select";
	public static final String FROM="from";
	public static final String JOIN="join";
	public static final String WHERE="where";
	public static final String GROUP="group by";
	public static final String ORDER="order by";
	public static final String LIMIT="limit";
	
	public static final String MATRIX="matrix";
	public static final String DISTINCT="distinct";
	
	
	// MapReduce使用
	public static final String MR_REDUCE_FORMAT="#mr.reduce.format";
	public static final String MR_REDUCE_MATRIX="#mr.reduce.matrix";
	public static final String MR_REDUCE_SELECT="#mr.reduce.select";
	
	public static final String MR_SORT_FORMAT="#mr.sort.format";
	
	/**
	 * 存放SQL解析的结果
	 */
	private Map<String, String> SQL = new HashMap<String, String>();

	/**
	 * 解析SQL
	 * 
	 * @param sql
	 */
	public SqlParse(String sql) {

		// 格式化SQL
		sql = sql
				.trim()
				.replaceAll("\\s*(create|select|from|join|where|group by|order by|limit)\\s+","}$0{")
				.substring(1)
				.concat("}")
				.replaceAll("\\s*(,|>|<|=|!=|>=|<=|like)\\s*", "$1");

		// 拆分SQL
		Pattern p = Pattern
				.compile("(create|select|from|join|where|group by|order by|limit)\\s+\\{(.*?)\\}");
		Matcher m = p.matcher(sql);

		// 将正则匹配的结果存放到Map中
		while (m.find()) {
			String func = m.group(1);
			String param = m.group(2);

			// 处理多个join的时候
			SQL.put(func, SQL.get(func) == null ? param : SQL.get(func) + "|"
					+ param);
		}
		
		// 从SQL中解析出 matrix和distinct
		String select=SQL.get(SELECT);
		String[] splits = select.split(",");
		
		String matrix="";
		String distinct=null;
		
		for (String field : splits) {
			if (field.matches(".*(sum|avg|max|min|count).*")) {
				matrix +=field+",";
			} else if (field.matches(".*distinct.*")) {
				distinct = field;
			} 
		}
		
		if (!matrix.equals("")) {
			matrix = matrix.substring(0,matrix.length()-1);
		}else{
			matrix=null;
		}
		SQL.put(MATRIX,matrix);
		SQL.put(DISTINCT,distinct);
		
		
		// 格式化where语句
		String where=SQL.get(WHERE);
		if(where!=null){
			where = where.replace("&&", "and").replace("||", "or")
					.replaceAll("\\(|\\)", " $0 ");
	
			String[] compare = { ">=", "<=", "=", "!=", "<", ">", "like" };
			for (int i = 0; i < compare.length; i++) {
				where = where.replaceAll("\\s*" + compare[i] + "\\s*", compare[i]);
			}
			SQL.put(WHERE,where);
		}
		
		// 主表和join表
		SQL.put(MAIN_TABLE, this.getMainTable());
		SQL.put(JOIN_TABLE, this.getJoinTables());
		SQL.put(OUTPUT,this.getOutPath());
		SQL.put(INPUT,this.getInputPath());
		
		// 用于MapReduce中
		String mtxReg="([s,a,m,n,c])(um|vg|ax|in|ount)\\s*\\((.*?)\\)\\s+as\\s+([\\w|\\.]+)";
		if(matrix!=null){
			SQL.put(MR_REDUCE_FORMAT,matrix.replaceAll(mtxReg,"$1#$3"));
			SQL.put(MR_REDUCE_MATRIX,matrix.replaceAll(mtxReg,"$1$2($1#$3) as $4"));
		}
		SQL.put(MR_REDUCE_SELECT,select.replaceAll(mtxReg,"$1$2($1#$3) as $4"));
		SQL.put(MR_SORT_FORMAT,select.replaceAll(mtxReg,"$4"));
		
		
	}
	/**
	 * 获取SQL中的value
	 * 
	 * @param key
	 * @return
	 */
	public String get(String key) {
		return SQL.get(key);
	}
	/**
	 * 获取SQL中的value
	 * 
	 * @param key
	 * @return
	 */
	public String get(String key,String def) {
		String val=SQL.get(key);
		return val==null?def:val;
	}

	/**
	 * 解析distinct字段
	 * @param distinct
	 * @return
	 */
	public static String getDistinct(String distinct){
		String regx="distinct\\s+\\((.*?)\\)";
		Pattern p = Pattern.compile(regx);
		Matcher m = p.matcher(distinct);
		String col = null;
		if (m.find()) {
			col = m.group(1);
		}
		
		return col;
	}
	/**
	 * 解析Select中的聚合函数
	 * @param mtrix
	 * @param flag
	 * @return
	 */
	public static String getMetrix(String mtrix,String flag){
		
		if (!mtrix.contains("as")) {
			mtrix += " as matrix_name";
		}
		Pattern p = Pattern
				.compile("(sum|avg|max|min|count)\\s*\\((.*?)\\)\\s+as\\s+([\\w|\\.]+)");
		Matcher m = p.matcher(mtrix);

		String func = null;
		String field = null;
		String alias = null;
		if (m.find()) {
			func = m.group(1);
			field = m.group(2);
			alias = m.group(3).trim();
			if (alias.equals("matrix_name")) {
				alias = func + "(" + field + ")";
			}
		}
		
		Map<String,String> res=new HashMap<String,String>();
		res.put("func",func);
		res.put("field",field);
		res.put("alias",alias);
	
		return res.get(flag);
	}

	
	/**
	 * 从table串中解析表名
	 * 
	 * @param table  eg:classpath:student.txt s
	 * @return
	 */
	public static String getTable(String table) {
		String[] splits = table.trim().split("\\s+");
		if (splits.length == 1) {
			return splits[0];
		} else {
			return splits[1];
		}
	}

	/**
	 * 从table串中解析表路径
	 * 
	 * @param table eg:classpath:student.txt s
	 * @return
	 */
	public static String getPath(String table) {
		String[] splits = table.trim().split("\\s+");
		if (splits.length == 1) {
			return null;
		} else {
			return splits[0];
		}
	}
	
	
	/**
	 * 返回需要join的表的表名
	 * 
	 * @return
	 */
	private String getJoinTables(){
		String joins="";
		String join = this.get("join");
		if (join != null) {
			for (String en : join.split("\\|")) {
				String table = en.split("on")[0];
				String name = SqlParse.getTable(table);
				joins=name+",";
			}
		}
		if(!joins.equals("")){
			joins=joins.substring(0,joins.length()-1);
		}
		return joins;
	}
	/**
	 * 返回主表名称
	 * 
	 * @return
	 */
	private String getMainTable(){
		String from = this.get("from");
		return SqlParse.getTable(from);
	}

	/**
	 * 返回输出目录
	 * @return
	 */
	private String getOutPath(){
		String create =this.get("create");
		if(create!=null){
			create=create.split("\\s+")[0];
		}
		return create;
	}
	/**
	 * 返回输入目录 
	 * eg:input:name,input:name
	 * @return
	 */
	private String getInputPath(){
		
		String input=null;
		
		String regex="(\\S+)\\s*(as)?\\s*(\\S*)";
		String replace="$1:$3";
		
		String from=this.get("from");
		String joins=this.get("join");
		
		if(from!=null){
			String tmp=from.trim().replaceAll(regex,replace);
			input=(tmp.endsWith(":")?tmp.substring(0,tmp.length()-1):tmp)+",";
		}
		if (joins != null) {
			for (String join : joins.split("\\|")) {
				String table = join.split("on")[0];
				String tmp=table.trim().replaceAll(regex,replace);
				input+=(tmp.endsWith(":")?tmp.substring(0,tmp.length()-1):tmp)+",";
			}
		}
		if(input!=null) input=input.substring(0, input.length()-1);
		return input;
	}
}
