package com.fearsfx.is.awesome;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Dump {

	static String root = "root", databaseName = "liybo", fileName = "liybo.txt";
	static List<String> table_queries = new ArrayList<String>();
	static List<String> relation_queries = new ArrayList<String>();
	static List<String> insert_queries = new ArrayList<String>();
	static List<String> UNIQUES = new ArrayList<String>();

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		createDatabase();

		generateCreates();

		createTables();

		generateRelations();

		createRelations();

		generateInserts();

		doInserts();

		dumpDatabase(root, root, databaseName);

		doMigration();

		dumpDatabase2(root, root, databaseName);

		System.out.println("end??");
	}

	@SuppressWarnings("resource")
	public static void doMigration() throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String line;
		while ((line = br.readLine()) != null) {
			if (line.contains("Separate")) {
				String table = line.split(" ")[1];

				BufferedReader br2 = br;
				String next = br2.readLine();
				String newTable1 = next.split(" ")[0];
				String contTable1 = next.split(" ")[2];
				next = br2.readLine();
				String newTable2 = next.split(" ")[0];
				String contTable1type = "";

				ArrayList<String> contTable2 = new ArrayList<String>();
				ArrayList<String> contTable2types = new ArrayList<String>();

				ArrayList<Object> content1 = new ArrayList<Object>();
				ArrayList<Object> content2 = new ArrayList<Object>();

				String dbUrl = "jdbc:mysql://localhost:3306/" + databaseName;
				String dbDriverClass = "com.mysql.jdbc.Driver";
				String userName = "root", password = "root";

				try {
					Class.forName(dbDriverClass);
					Connection conn = DriverManager.getConnection(dbUrl,
							userName, password);

					Statement st = conn.createStatement();

					ResultSet rs = st.executeQuery("select * from " + table);
					ResultSetMetaData rsmd = rs.getMetaData();
					for (int i = 2; i < rsmd.getColumnCount() + 1; i++) {
						if (!rsmd.getColumnName(i).equals(contTable1)) {
							contTable2.add(rsmd.getColumnName(i));
							contTable2types.add(rsmd.getColumnTypeName(i));
						} else {
							contTable1type = rsmd.getColumnTypeName(i);
							if (contTable1type.equals("VARCHAR"))
								contTable1type += "(255)";
						}
					}

					String query1 = "CREATE TABLE IF NOT EXISTS " + newTable1
							+ " (id INT NOT NULL AUTO_INCREMENT, " + contTable1
							+ " " + contTable1type + ", PRIMARY KEY(id))";
					String query2 = "CREATE TABLE IF NOT EXISTS " + newTable2
							+ " (id INT NOT NULL AUTO_INCREMENT";

					for (int i = 0; i < contTable2.size(); i++) {
						query2 += ", " + contTable2.get(i) + " ";
						if (contTable2types.get(i).equals("VARCHAR"))
							query2 += contTable2types.get(i) + "(255)";
						else
							query2 += contTable2types.get(i);
					}
					query2 += ", PRIMARY KEY(id))";

					st.executeUpdate(query1);
					st.executeUpdate(query2);

					System.out.println(query1);
					System.out.println(query2);

					rs = st.executeQuery("select * from " + table);
					while (rs.next()) {

						for (int i = 2; i < rsmd.getColumnCount() + 1; i++) {
							if (!rsmd.getColumnName(i).equals(contTable1)) {
								content2.add(rs.getObject(i));
							} else {
								content1.add(rs.getObject(i));
							}
						}
					}
					int pos = 0;
					for (int i = 0; i < content1.size(); i++) {
						String query3 = "INSERT INTO " + newTable1
								+ " VALUES (NULL, " + "'" + content1.get(i)
								+ "'" + ")";
						String query4 = "INSERT INTO " + newTable2
								+ " VALUES (NULL";
						for (int j = 0; j < contTable2.size(); j++, pos++) {
							if (contTable2types.get(j).equals("VARCHAR")
									|| contTable2types.get(j).equals("DATE")) {
								query4 += ", '" + content2.get(pos) + "'";
							} else {
								query4 += ", " + content2.get(pos);
							}
						}
						query4 += ")";
						System.out.println(query3);
						System.out.println(query4);
						st.executeUpdate(query3);
						st.executeUpdate(query4);
					}
					// st.executeUpdate("DROP TABLE Article");

				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static void doInserts() {

		String dbUrl = "jdbc:mysql://localhost:3306/" + databaseName;
		String dbDriverClass = "com.mysql.jdbc.Driver";
		String userName = "root", password = "root";

		for (String s : insert_queries)
			try {
				Class.forName(dbDriverClass);
				Connection conn = DriverManager.getConnection(dbUrl, userName,
						password);
				Statement st = conn.createStatement();
				st.executeUpdate(s);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}

	}

	public static void generateInserts() {

		String dbUrl = "jdbc:mysql://localhost:3306/" + databaseName;
		String dbDriverClass = "com.mysql.jdbc.Driver";
		String userName = "root", password = "root";

		try {
			Class.forName(dbDriverClass);
			Connection conn = DriverManager.getConnection(dbUrl, userName,
					password);
			Statement st = conn.createStatement();

			DatabaseMetaData md = conn.getMetaData();

			ResultSet rs = md.getTables(null, null, "%", null);
			while (rs.next()) {

				ResultSet res = st.executeQuery("select * from "
						+ rs.getString("TABLE_NAME"));
				ResultSetMetaData rsmd = res.getMetaData();

				int columnCount = rsmd.getColumnCount();

				for (int a = 1; a < 6; a++) {
					String query = "INSERT INTO " + rs.getString("TABLE_NAME")
							+ " VALUES(";

					for (int i = 1; i < columnCount + 1; i++) {
						String type = rsmd.getColumnTypeName(i);
						if (type.equals("INT"))
							if (i == 1)
								query += "NULL";
							else {
								if (UNIQUES.size() != 0) {
									if (UNIQUES.contains(rsmd.getColumnName(i))) {
										query += ", " + a;
									} else {
										if (a < 3)
											query += ", " + 1;
										if (a == 3)
											query += ", " + 2;
										if (a > 3)
											query += ", " + 3;
									}
								} else {
									if (a < 3)
										query += ", " + 1;
									if (a == 3)
										query += ", " + 2;
									if (a > 3)
										query += ", " + 3;
								}
							}
						if (type.equals("FLOAT") || type.equals("DOUBLE"))
							query += ", " + new Float(a);
						if (type.equals("DATE"))
							query += ", NOW()";
						if (type.equals("VARCHAR"))
							switch (a) {
							case 1:
								query += ", \"AAAA\"";
								break;
							case 2:
								query += ", \"BBBB\"";
								break;
							case 3:
								query += ", \"CCCC\"";
								break;
							case 4:
								query += ", \"DDDD\"";
								break;
							case 5:
								query += ", \"EEEE\"";
								break;
							}
					}
					query += ")";
					insert_queries.add(query);
					System.out.println(query);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

	public static void createRelations() {

		String dbUrl = "jdbc:mysql://localhost:3306/" + databaseName;
		String dbDriverClass = "com.mysql.jdbc.Driver";
		String userName = "root", password = "root";

		for (String s : relation_queries)
			try {
				Class.forName(dbDriverClass);
				Connection conn = DriverManager.getConnection(dbUrl, userName,
						password);
				Statement st = conn.createStatement();
				st.executeUpdate(s);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}

	}

	public static void generateRelations() throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String line;
		while ((line = br.readLine()) != null) {
			if (line.contains("connection")) {

				String rels[] = { " has a one to one connection to ",
						" has a one to many connection to ",
						" has a many to one connection to ",
						" has a many to many connection to " };

				int i = 0;
				for (; !line.contains(rels[i]); i++) {
				}
				switch (i) {
				case 0: {
					String query = "ALTER TABLE " + line.split(rels[i])[0]
							+ " ADD " + line.split(rels[i])[1] + "_id int";
					relation_queries.add(query);
					System.out.println(query);
					query = "ALTER TABLE " + line.split(rels[i])[0]
							+ " ADD UNIQUE (" + line.split(rels[i])[1] + "_id)";
					UNIQUES.add(line.split(rels[i])[1] + "_id");
					relation_queries.add(query);
					System.out.println(query);
				}
					break;
				case 1: {
					String query = "ALTER TABLE " + line.split(rels[i])[1]
							+ " ADD " + line.split(rels[i])[0] + "_id int";
					relation_queries.add(query);
					System.out.println(query);
				}
					break;
				case 2: {
					String query = "ALTER TABLE " + line.split(rels[i])[0]
							+ " ADD " + line.split(rels[i])[1] + "_id int";
					relation_queries.add(query);
					System.out.println(query);
				}
					break;
				case 3: {
					String query = "CREATE TABLE " + line.split(rels[i])[0]
							+ "_to_" + line.split(rels[i])[1]
							+ "(id int AUTO_INCREMENT, "
							+ line.split(rels[i])[0] + "_id int, "
							+ line.split(rels[i])[1]
							+ "_id int, PRIMARY KEY (id))";
					relation_queries.add(query);
					System.out.println(query);
				}
					break;
				}
			}
		}
		br.close();

	}

	public static void createTables() {

		String dbUrl = "jdbc:mysql://localhost:3306/" + databaseName;
		String dbDriverClass = "com.mysql.jdbc.Driver";
		String userName = "root", password = "root";

		for (String s : table_queries)
			try {
				Class.forName(dbDriverClass);
				Connection conn = DriverManager.getConnection(dbUrl, userName,
						password);
				Statement st = conn.createStatement();
				st.executeUpdate(s);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}

	}

	public static void createDatabase() {

		String dbDriverClass = "com.mysql.jdbc.Driver";

		try {
			Class.forName(dbDriverClass);
			Connection conn = DriverManager
					.getConnection("jdbc:mysql://localhost/?user=root&password=root");
			Statement st = conn.createStatement();
			st.executeUpdate("CREATE DATABASE IF NOT EXISTS " + databaseName);
		} catch (SQLException e) {
			// e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void generateCreates() throws IOException {

		List<String> columns = new ArrayList<String>(), types = new ArrayList<String>();

		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String line;
		while ((line = br.readLine()) != null) {
			String que[] = line.split("Create table");
			if (que.length != 1) {
				que = que[1].split(" ");
				String line2;
				BufferedReader br2 = br;
				line2 = br2.readLine();
				{
					String row[] = line2.split(",\\s+");
					int a = 0;
					while (a < row.length) {
						row[a] = row[a].replaceAll("\\s+", "");
						row[a] = row[a].replaceAll(",", "");
						String col[] = row[a].split(":");
						columns.add(col[0]);
						if (col[1].equals("integer"))
							col[1] = "int";
						if (col[1].equals("currency"))
							col[1] = "float";
						if (col[1].equals("string"))
							col[1] = "varchar(100)";
						if (col[1].equals("longstring"))
							col[1] = "varchar(200)";
						if (col[1].equals("longtext"))
							col[1] = "varchar(255)";
						if (col[1].equals("varchar"))
							col[1] = "varchar(50)";
						types.add(col[1]);
						a++;
					}
				}

				String query = "CREATE TABLE IF NOT EXISTS " + que[1]
						+ "(id int AUTO_INCREMENT";
				for (int i = 0; i < columns.size(); i++) {
					query += ", " + columns.get(i) + " " + types.get(i);
				}
				query += ", PRIMARY KEY (id))";
				table_queries.add(query);
				System.out.println(query);
				columns = new ArrayList<String>();
				types = new ArrayList<String>();
			}
		}
		br.close();
	}

	public static void dumpDatabase(String aUser, String aPasswd,
			String aDatabase) throws IOException {

		String executeCmd = String.format(
				"mysqldump -u %s -p%s --database %s -r %s_dump.sql", aUser,
				aPasswd, aDatabase, aDatabase);

		Process runtimeProcess;
		try {
			runtimeProcess = Runtime.getRuntime().exec(executeCmd);
			int processComplete = runtimeProcess.waitFor();

			if (processComplete == 0)
				System.out.println("Exported.");
			else
				System.out.println("Failed to export.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void dumpDatabase2(String aUser, String aPasswd,
			String aDatabase) throws IOException {

		String executeCmd = String.format(
				"mysqldump -u %s -p%s --database %s -r %s_dump2.sql", aUser,
				aPasswd, aDatabase, aDatabase);

		Process runtimeProcess;
		try {
			runtimeProcess = Runtime.getRuntime().exec(executeCmd);
			int processComplete = runtimeProcess.waitFor();

			if (processComplete == 0)
				System.out.println("Exported.");
			else
				System.out.println("Failed to export.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}