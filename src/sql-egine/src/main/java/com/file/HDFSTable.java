package com.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.hdfs.HDFSHelper;


/**
 * 文件到表格的映射对象
 * 
 * @author 魏国兴
 *
 */
public class HDFSTable extends Table{
	
	public HDFSTable(Configuration conf,Table table){
		this.name=table.getName();
		this.split=table.getSplit();
		this.format=table.getFormat();
		this.input=table.getInput();
		try {
			super.setRows(HDFSHelper.readLines(conf, this.input));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
