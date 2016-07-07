package com.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import com.conf.SqlConf;

/**
 * 从文件中加载Table
 * 
 * @author 魏国兴
 *
 */
public class FileTable extends Table {

	
	public FileTable(String input) {
		this(null, input, null, null, null);
	}

	public FileTable(String name, String input) {
		this(name, input, null, null, null);
	}

	public FileTable(String name, String input, String split, String format) {
		this(name, input, split, format, null);
	}

	public FileTable(String name, String split, String format, List<String> rows) {
		this(name, null, split, format, rows);
	}

	public FileTable(String name, String input, String split, String format,
			List<String> rows) {

		if (name == null) {
			name = new File(input).getName();
			if (name.lastIndexOf(".") > -1) {
				name = name.substring(0, name.lastIndexOf("."));
			}
		}
		if(input.startsWith("classpath:")){
			input=getResource(input);
		}
		if (split == null)
			split = this.readSplit(input);
		if (format == null)
			format = this.readFormat(input);
		
		this.name = name;
		this.input = input;
		this.split = split;
		this.format = format;
		if (rows != null) {
			this.addRows(rows);
		}
		this.addRows(this.readRows(input));

	}

	/**
	 * 从文件中获取日志格式
	 * 
	 * @param path
	 * @return
	 */
	public String readFormat(String path) {
		String format = null;

		// 读取一行
		List<String> rows = readFile(path, null, 1);

		if (rows != null) {
			format = rows.get(0);
			if (format.startsWith("#")) {
				format = format.substring("#".length());
			} else {
				String spl = this.readSplit(path);
				String splits[] = format.split(spl);
				String fmt = "";
				for (int i = 1; i <= splits.length; i++) {
					fmt += "col" + i + spl;
				}
				if (!fmt.equals("")) {
					format = fmt.substring(0, fmt.length() - 1);
				}
			}
		}
		return format;
	}

	/**
	 * 从文件中获取分隔串
	 * 
	 * @param path
	 * @return
	 */
	public String readSplit(String path) {

		String split = "|";

		// 从文件中读取3行
		List<String> rows = readFile(path, null, 2);

		String header = rows.get(0);
		String line = rows.get(1);

		// 尝试使用常用分隔符切分记录
		String[] splits = { "\\|", ",", "\\s+", "-", "_", ":" };

		for (String regex : splits) {
			int len1 = header.split(regex).length;
			int len2 = line.split(regex).length;

			if (len1 != 0 && len1 == len2) {
				if (regex.equals("\\s+")) {
					split = " ";
				}
				if (regex.equals("\\|")) {
					split = "|";
				} else {
					split = regex;
				}
				break;
			}
		}
		// 返回分隔符
		return split;
	}

	/**
	 * 从文件中获取记录数
	 * 
	 * @param path
	 * @return
	 */
	public List<String> readRows(String path) {
		// 读取所有非#号开头的行
		return this.readFile(path, "[^#].*", -1);
	}

	/**
	 * 读取文件指定行数
	 * 
	 */
	private List<String> readFile(String path, String filter, int num) {

		// 读取文件内容到List中
		List<String> rows = new ArrayList<String>();
		// 读取0行
		if (num == 0)
			return rows;
		try {
			// 获取文件的输入流
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(URLDecoder.decode(path, "UTF-8")),
					"UTF-8"));
			// 记录行数
			int counter = 0;
			// 读取文件
			String line = null;
			while ((line = reader.readLine()) != null) {
				// 读取指定行数
				if (num > 0 && counter++ > num) {
					break;
				} else {
					if (filter == null
							|| (filter != null && line.matches(filter))) {
						rows.add(line);
					}
				}
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rows.size() == 0 ? null : rows;
	}
	private String getResource(String resource) {
		String path = SqlConf.class.getClassLoader().getResource(resource.substring("classpath:".length()))
				.getPath();
		try {
			path = URLDecoder.decode(path, "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return path;
	}
}
