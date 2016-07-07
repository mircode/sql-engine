package com.file;

import static org.junit.Assert.*;

import org.junit.Test;

public class FileTableTest {

	@Test
	public void testFileTable(){
		
		String path=FileTableTest.class.getClassLoader().getResource("student.txt").getPath().toString();
		System.out.println(path);
		
		Table table=new FileTable(path);
		assertEquals("表格名称","student",table.getName());
		
		System.out.println(table.getName());
		System.out.println(table);
		

		String line=table.getRows().get(0);
		String column=table.getColumn(line, 1);
		System.out.println("获取第一列:"+column);
		
		column=table.getColumns(line, "*");
		System.out.println("获取所有列:"+column);
		
		column=table.getColumns(line, "name,grade");
		System.out.println("获取名称和成绩列:"+column);
		
		
	}
	
	
}
