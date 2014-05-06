package gmapper.mysql;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 *  Class Name : MySqlAdapter 
 *  @author Jinho Park, Steve Kim
 *	
 *	This class is responsible for 
 *	1) connect to mysql database
 *	2) monitor queue table
 *	3) retrieve the records from source table from the record in the queue
 *	4) generate a message to send to mapper
 *	5) send a message to mapper for processing
 *
 *	6) listen to admin queue for admin task
 *		6-1) create new mysql adapter instance
 *		6-2) destroy mysql adapter instance
 *		6-3) restart mysql adapter instance
 */
public class MySqlAdapter {
	
private static String configFile;
	private Connection adminConn = null;
	private Connection toMapperConn = null;
	private String qNameIncoming;
	private String qNameOutgoing;
	private String dbUrl;
	private String dbUser;
	private String dbPwd;
	
//	private static Channel adminChannel = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if (args.length == 1){
			configFile = args[0];
		}else{
			configFile = "config.properties";
		}
		
		System.out.println("MySql Adapter is launching...");

		MySqlAdapter adapter = new MySqlAdapter();
		adapter.run();
	}
	
	private void run() {
		// thread that monitors admin queue
		//AdminThread adminThread = new AdminThread("MySql Adapter Admin Thread", adminChannel);
		AdminThread adminThread = new AdminThread("MySql Adapter Admin Thread",adminConn,qNameIncoming,toMapperConn,qNameOutgoing,dbUrl,dbUser,dbPwd);
		adminThread.start();
		
		System.out.println("MySql Adapter is now running.");		
	}

	/** constructor
	 * 
	 * @param mapperq output queue to mapper
	 */
	public MySqlAdapter(){
		// Read configuration file and load the parameters.
		Properties prop = new Properties();
		InputStream input = null;
	 
		try {
	 
			input = new FileInputStream(configFile);
	 
			// load a properties file
			prop.load(input);
	 
			// get the property value and print it out
			String qUriIncoming = prop.getProperty("Q_URI_INCOMING");
			String qUserIncoming = prop.getProperty("Q_USER_INCOMING");
			String qPwdIncoming = prop.getProperty("Q_PASSWORD_INCOMING");
			qNameIncoming = prop.getProperty("Q_NAME_INCOMING");
			
			String qUriOutgoing = prop.getProperty("Q_URI_OUTGOING");
			String qUserOutgoing = prop.getProperty("Q_USER_OUTGOING");
			String qPwdOutgoing = prop.getProperty("Q_PASSWORD_OUTGOING");
			qNameOutgoing = prop.getProperty("Q_NAME_OUTGOING");
			
			dbUrl = prop.getProperty("JDBC_URL");
			dbUser = prop.getProperty("JDBC_USER");
			dbPwd = prop.getProperty("JDBC_PASSWORD");
			
			if (input!=null)
				input.close();
			
			ConnectionFactory factoryIncoming = new ConnectionFactory();
			URI uriIncoming = new URI(qUriIncoming);
				
			factoryIncoming.setUri(uriIncoming);
			factoryIncoming.setUsername(qUserIncoming);
			factoryIncoming.setPassword(qPwdIncoming);
			factoryIncoming.setRequestedChannelMax(0);
			
			adminConn = factoryIncoming.newConnection();
			
			ConnectionFactory factoryOutgoing = new ConnectionFactory();
			URI uriOutgoing = new URI(qUriOutgoing);
				
			factoryOutgoing.setUri(uriOutgoing);
			factoryOutgoing.setUsername(qUserOutgoing);
			factoryOutgoing.setPassword(qPwdOutgoing);
			factoryOutgoing.setRequestedChannelMax(0);
			
			toMapperConn = factoryOutgoing.newConnection();
			
			
			
			//pass connection
//			adminChannel = adminQ.createChannel();
//			adminChannel.exchangeDeclare("adminQueue","", true, false, false, null);
//			adminChannel.basicQos(0); // maximum number of messages that the server will deliver. 0: unlimited
//		
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (KeyManagementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	
}
