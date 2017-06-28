import java.net.URISyntaxException;
import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import org.apache.qpid.client.AMQAnyDestination;
import org.apache.qpid.client.AMQConnection;

public class QpidConnectionTest {

    public static void main(String []args) {        
        String ip = "218.4.33.212";
        String userId = "app0";
        String port = "4703";
        String queueId = "app0.849dc093b4eb493383e3c034b4fe086d.78c122b7";
        System.out.println("amqp started:" + "userId:" + userId + ",queueId:" + queueId + ",ip:" + ip + ",port:" + port);

        String path = "/product/14/github/test.qpidclient/cert/";
        // 设置系统环境jvm参数
        System.setProperty("javax.net.ssl.keyStore", path + "user.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "importkey");
        System.setProperty("javax.net.ssl.trustStore", path + "root.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "importkey");

        // 地址变量  
        String brokerOpts = "?brokerlist='tcp://"+ip+":"+port+"?ssl='true'&ssl_verify_hostname='false''";
        String connectionString = "amqp://"+userId+":"+"xxxx@xxxx/"+brokerOpts;
        System.out.println("connection string:" + connectionString);
        // 建立连接
        AMQConnection conn = null;
        try {
            conn = new AMQConnection(connectionString);
            conn.start();
            // 获取session
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // 获取队列
            Destination queue = new AMQAnyDestination("ADDR:"+queueId+";{create:sender}");
            MessageConsumer consumer = session.createConsumer(queue);

            while(true) {
                Message m = consumer.receive(60000);
                // message为空的情况,
                if(m == null){
                    System.out.println("Get NULL message, pause for 1 miniute!");
                    //                    sleep(60000);
                    continue;
                }
                // message格式是否正确
                if(m instanceof BytesMessage) {
                    BytesMessage tm = (BytesMessage)m;
                    int length = new Long(tm.getBodyLength()).intValue();
                    if(length > 0){
                        byte[] keyBytes = new byte[length];
                        tm.readBytes(keyBytes);
                        String messages = new String(keyBytes);
                        System.out.println(messages);
                    }else{
                        System.out.println("Get zero length message");
                    }
                }else{
                    System.out.println("Message is not in Byte format!");
                }
            }
        } catch (Exception e){
            System.out.println(e);
        } finally{
            try {
                if(conn != null)
                {
                    conn.close();
                }
            } catch (Exception e) {
                System.out.println(e);
            }
            System.out.println("[AMQP]No data from SVA,connection closed!");
        }
    }
}
