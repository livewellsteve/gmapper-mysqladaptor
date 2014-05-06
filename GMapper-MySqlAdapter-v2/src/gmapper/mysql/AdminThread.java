/**
 * 
 */
package gmapper.mysql;


import gmapper.mysql.ProtobufAdminQMsg.AdminQMsg;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;

/**
 * @author mac
 *
 */
public class AdminThread implements Runnable {

	private Thread t;
	private String threadNameT;	// or ID of the thread
	private Connection adminConn;
	private Connection toMapperConn;
	private String qNameIncoming;
	private String qNameOutgoing;
	private String dbUrl;
	private String dbUser;
	private String dbPwd;
	
	private Map<String,Thread> mysqlInstance;
	   
	public AdminThread(String threadNameT,Connection adminConn,String qNameIncoming,Connection toMapperConn,String qNameOutgoing, String dbUrl, String dbUser, String dbPwd){
	       this.threadNameT = threadNameT;
	       this.adminConn = adminConn;
	       this.qNameIncoming = qNameIncoming;
	       this.toMapperConn = toMapperConn;
	       this.qNameOutgoing = qNameOutgoing;
	       this.dbUrl = dbUrl;
	       this.dbUser = dbUser;
	       this.dbPwd = dbPwd;
	       	       
		   System.out.println("Creating " +  threadNameT );
	       
	       mysqlInstance = new HashMap<String,Thread>();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		
		try {
			Channel adminChannel = adminConn.createChannel();
			QueueingConsumer consumer = new QueueingConsumer(adminChannel);
			adminChannel.basicConsume(qNameIncoming, true, consumer);

			while(true) {
					
				// read admin message queue for following actions
			    QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			    
			    if (delivery != null){
			    ////need to check below
			    ByteArrayInputStream iStream = new ByteArrayInputStream(delivery.getBody());
			   AdminQMsg adminQMsg = AdminQMsg.parseFrom(iStream);
			      
			    String action = adminQMsg.getAction();
			    //name of thread 
			    String adapterThreadName = adminQMsg.getTableId()+"|"+adminQMsg.getSourceOrTarget()+"|"+adminQMsg.getTriggerId();
			    
			    System.out.println(" [x] Received '" + action + "'");   
			    
				if (action != null){
				
					if (action.equalsIgnoreCase("CREATE")) {
						// 1) create mysql adapter instance
						createMysqlInstance(adminQMsg,adapterThreadName,adminQMsg.getTableId());
					}else
					if (action.equalsIgnoreCase("DESTROY")) {
						// 2) destroy mysql adapter instance
						destroyMysqlInstance(adapterThreadName);
					}else
					if (action.equalsIgnoreCase("RESTART")) {
						// 3) restart mysql adapter instance
						restartMysqlInstance(adminQMsg,adapterThreadName,adminQMsg.getTableId());
					}
					
				}
	
			    System.out.println(" [x] Done" );
			    //adminChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			    }
			
					Thread.sleep(3 * 1000);
				
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	private void createMysqlInstance(AdminQMsg adminQMsg, String adapterTheadName, String listenerTableName) {
		// query mapper config from main DB
		// query mapper field config from main DB (add to mapped field list)
		// 1) ID, 2) host, 3) username, 4) password....etc.
		System.out.println("in createinstance method");
		Thread t = new Thread(new MySqlAdapterThread(adminQMsg, toMapperConn, qNameOutgoing,dbUrl,dbUser,dbPwd, listenerTableName));
		t.start();
		mysqlInstance.put(adapterTheadName, t);
		
		// TODO: Logging
	}

	private void destroyMysqlInstance(String adapterThreadName) {
		// find the correct thread from the list and stop/destroy it

		Thread t = mysqlInstance.get(adapterThreadName);
		t.interrupt();
		///TODO: DELETE TRIGGER AND TABLE from source db! if sourceOrTarget is source
		mysqlInstance.remove(adapterThreadName);
		// TODO: Logging
	}

	private void restartMysqlInstance(AdminQMsg adminQMsg,String adapterThreadName,String listenerTableName) {
		// find the correct thread from the list and restart it 

		destroyMysqlInstance(adapterThreadName);
		createMysqlInstance(adminQMsg,adapterThreadName, listenerTableName);
		// TODO: Logging
	}


	public void start()
	{
		System.out.println("Starting " + threadNameT);

		if (t == null)
		{
			
			t = new Thread (this, threadNameT);
			t.run();
		}
	}
}
