package gmapper.mysql;

import gmapper.mysql.ProtobufAdminQMsg.AdminQMsg;
import gmapper.mysql.ProtobufMapperQMsg.MapperQMsg;

import java.io.ByteArrayOutputStream;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class MySqlAdapterThread implements Runnable {

	String threadid;
	String sourceOrTarget; // SOURCE - TARGET
	Connection toMapperConn = null;
	String qNameOutgoing;
	String dbUrl;
	String dbUser;
	String dbPwd;
	String sourceUrl;
	String sourceUser;
	String sourcePwd;
	String sourceObj;
	String sourcePk;
	String listenerTableName;
	int mapperId;
	List<String> fieldName;
	List<String> fieldType;
	List<String> fieldValue;

	public MySqlAdapterThread(AdminQMsg adminQMsg, Connection toMapperConn,
			String qNameOutgoing, String dbUrl, String dbUser, String dbPwd, String listenerTableName) {
		// todo: change parameters as needed.

		fieldName = new ArrayList<String>();
		fieldType = new ArrayList<String>();
		fieldValue = new ArrayList<String>();
		
		sourceOrTarget = adminQMsg.getSourceOrTarget();
		this.toMapperConn = toMapperConn;
		this.qNameOutgoing = qNameOutgoing;
		this.dbUrl = dbUrl;
		this.dbUser = dbUser;
		this.dbPwd = dbPwd;
		this.listenerTableName = listenerTableName;
		mapperId = adminQMsg.getMapperId();

		readMapperDb(mapperId, sourceOrTarget);
		
		System.out.println("in constructor thread, sourceURL: "+sourceUrl);
		
		
		
		

	}

	private void readMapperDb(int mapperid, String sourceOrTarget) {
		// TODO: query mapper config
		// SELECT source adaptor type, source adapter connection, source adapter
		// userid, source adapter password,....
		// FROM mapperconfig
		// WHERE mapperid = mapperid

		if (sourceOrTarget.equalsIgnoreCase("source")) {
			String query = "SELECT * FROM adaptor_access "
					+ "INNER JOIN mapper_config ON adaptor_access.adaptor_id = mapper_config.source_adaptor_id "
					+ "INNER JOIN mapper_field_config ON mapper_config.mapper_id = mapper_field_config.mapper_id ";

			java.sql.Connection dbConn = null;
			ResultSet rs;
			try {
				Class.forName("com.mysql.jdbc.Driver");
			} catch (ClassNotFoundException e) {
				System.out.println("driver not load");
			}
			try {
				dbConn = DriverManager.getConnection(dbUrl, dbUser, dbPwd);
				Statement stmt = dbConn.createStatement();
				rs = stmt.executeQuery(query);
				while (rs.next()) {
				
					fieldName.add(rs.getString("mapper_field_config.source_field_name"));
					fieldType.add(rs.getString("mapper_field_config.source_field_type"));
					sourceObj = rs.getString("mapper_config.source_object");
					sourceUrl = "jdbc:mysql://"
							+ rs.getString("adaptor_access.endpoint") + "/"
							+ rs.getString("adaptor_access.db_name");
					sourceUser = rs.getString("adaptor_access.user");
					sourcePwd = rs.getString("adaptor_access.password");
					sourcePk = rs.getString("mapper_config.source_pk");
				}
				if (stmt != null)
					stmt.close();
				if (dbConn != null)
					dbConn.close();
			} catch (SQLException e) {
				System.out.println("connection failed from: readMapperDb");
			}
			
		}

	}

	@Override
	public void run() {
		while (!Thread.currentThread().isInterrupted()) {

			// TODO: read queue table from mysql database (If
			// isSourceOrTarget.equals("SOURCE") then do this)

			// ResultSet rs = readQueue()
			// if(rs != null){
			// query config field from Mapper Field Config for given Mapper
			// Select fieldconfig.Field1, fieldconfig.Field2,...etc FROM
			// mapperconfig.SourceTable WHERE mapperconfig.source_pk_field =
			// queue.pkvalue

			// from the query from above
			// generate protobuf message

			// publish message to rabbitMQ (TO_MAPPER_Q = IncomingQ)
			// }

			if (sourceOrTarget.equalsIgnoreCase("source")) {
				java.sql.Connection sourceConn = null;
				ResultSet rs;
				String sql = "SELECT trigger_id,table_name,pk_val,time,mapper_field_id FROM "
						+ listenerTableName;
				//test
				System.out.println(sql+" IN thread run");
				
				try {
					sourceConn = DriverManager.getConnection(sourceUrl,
							sourceUser, sourcePwd);
					Statement stmt = sourceConn.createStatement(
							ResultSet.TYPE_FORWARD_ONLY,
							ResultSet.CONCUR_UPDATABLE);
					rs = stmt.executeQuery(sql);
					while (rs.next()) {
						String tableName = rs.getString("table_name");// dont
																		// need
						String pkVal = rs.getString("pk_val");
						Date time = rs.getTimestamp("time");// dont need

						java.sql.Connection conn2 = DriverManager
								.getConnection(sourceUrl, sourceUser, sourcePwd);
						Statement stmt2 = conn2.createStatement();
						String sql2 = "SELECT * FROM " + tableName + " WHERE "
								+ sourcePk + "=" + "'" + pkVal + "'";
						
						//testing
						System.out.println(sql2+" sql for changes");
						
						ResultSet rs2 = stmt2.executeQuery(sql2);

						//need this in map
					while (rs2.next()) {
							for (int i =0; i< fieldType.size(); i++){
								
							if (fieldType.get(i).equalsIgnoreCase("varchar"))
								fieldValue.add(rs2.getString(fieldName.get(i)));
			
							else if (fieldType.get(i).equalsIgnoreCase("int") || fieldType.get(i).equalsIgnoreCase("integer") )
								fieldValue.add(String.valueOf(rs2.getInt(fieldName.get(i))));
			
							else if (fieldType.get(i).equalsIgnoreCase("double"))
								fieldValue.add(String.valueOf(rs2.getDouble(fieldName.get(i))));
							
							else if (fieldType.get(i).equalsIgnoreCase("date") || fieldType.get(i).equalsIgnoreCase("timestampe") )
								fieldValue.add(String.valueOf(rs2.getTime(fieldName.get(i))));
							}
							
						}
						if (rs2 != null)
							rs2.close();
						if (stmt2 != null)
							stmt2.close();
						if (conn2 != null)
							conn2.close();

						// PLACE TO Q!
						MapperQMsg mapperQMsg = ProtobufMapperQMsg.MapperQMsg.newBuilder()
								.addAllFieldName(fieldName)
								.addAllFieldType(fieldType)
								.addAllFieldValue(fieldValue)
								.setMapperId(mapperId)
								.build();
						pushToQ(mapperQMsg);

						//testing
						System.out.println(mapperQMsg.getFieldValueList());
						
						int rowNum = rs.getRow();
						//TODO:
						rs.deleteRow(); //instead of delete, create a column to mark used or not, and change the value once used
						if (rowNum == rs.getRow()) {
							rs.previous();
						}
					}
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
//					if (sourceConn != null)
//						sourceConn.close();
				} catch (SQLException e) {
					System.out.println("connection failed");
				}
			}

			// TODO: (If isSourceOrTarget.equals("TARGET") then do this)
			// read rabbitMQ for target data source request (FROM_MAPPER_Q =
			// OutgoingQ)
			// if there is any message from MQ

			// process that message and write to DB
			// 1) if message is "insert" action type, generate insert statement
			// from the message
			// 2) if message is "update" action type, generate update statement
			// from the message
			// source.PK => target.FK
			// 3) if message is "delete" action type, generate delete statement
			// from the message
			// source.PK => target.FK
			
			else if(sourceOrTarget.equalsIgnoreCase("source")){
				//fromMapperConn, create chanel, consume q, which is sql 
				//query mapperdb to get credential
				//execute sql
				
			}
			
			try {
				Thread.sleep(3 * 1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private void pushToQ(MapperQMsg mapperQMsg){
		
		try{
			Channel chan = toMapperConn.createChannel();
			ByteArrayOutputStream oStream = new ByteArrayOutputStream();
			mapperQMsg.writeTo(oStream);
			chan.basicPublish("", qNameOutgoing, null, oStream.toByteArray());

			if (chan != null)
				chan.close();
	
		}catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	

}
