package Cloud.Project2;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

public class SQS {

	
	public static AWSCredentials credentials = new BasicAWSCredentials("xxxxxxxxxxxx", "aaaaaaaaaaaaaaa");
	
	public static AmazonSQSClient amazonSQSClient=new AmazonSQSClient(credentials);
	
	public static String deleteQueue="";
	
	// creates two queues : csc470test and csc470test2
	public static String createQueue()
	{
		String queue="";
		try
		{
		CreateQueueResult createQueueResult= new CreateQueueResult();
				createQueueResult=amazonSQSClient.createQueue("csc470test");
				queue=createQueueResult.getQueueUrl();
		System.out.println(" Queue csc470test created Successfully");
				createQueueResult=amazonSQSClient.createQueue("csc470test2");
				System.out.println(" Queue csc470test2 created Successfully");
				deleteQueue=createQueueResult.getQueueUrl();
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    	catch(Exception e)
    	{
    		System.out.println("Error :" +e.getMessage());
    	}
		return queue;
	}
	
	//Displays the list of Available Queues
	public static void listQueue()
	{
		try
		{
			System.out.println("*** List of Available Queues ***");
		for (String  queue : amazonSQSClient.listQueues().getQueueUrls()) {
			System.out.println(queue);
		}
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    	catch(Exception e)
    	{
    		System.out.println("Error :" +e.getMessage());
    	}
	}
	
	//Get the Queue Attributes 
	public static void getQueueAttributes(String queue)
	{
		try
		{
		GetQueueAttributesRequest getQueueAttributesRequest=new GetQueueAttributesRequest();
		getQueueAttributesRequest.withQueueUrl(queue);
		ArrayList<String> attributes=new ArrayList<String>();
		attributes.add("All");
		getQueueAttributesRequest.setAttributeNames(attributes);
		GetQueueAttributesResult getQueueAttributesResult=amazonSQSClient.getQueueAttributes(getQueueAttributesRequest);
		Map<String, String> attributesList=getQueueAttributesResult.getAttributes();
		Iterator it = attributesList.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        if(pair.getKey().toString().equalsIgnoreCase("CreatedTimestamp"))
	        {
	        	Long time=Long.parseLong(pair.getValue().toString());
	        	Date dateObj = new Date(time*1000);
	        System.out.println(pair.getKey() + " = " + dateObj);
	        }
	        else if(pair.getKey().toString().equalsIgnoreCase("QueueArn"))
	        {
	        	System.out.println(pair.getKey() + " = " + pair.getValue());
	        }
	        else if(pair.getKey().toString().equalsIgnoreCase("MessageRetentionPeriod"))
	        {
	        	int day = (int)TimeUnit.SECONDS.toDays(Integer.parseInt(pair.getValue().toString()));  
	        System.out.println(pair.getKey() + " = " + day +" days" );
	        }
	        else if(pair.getKey().toString().equalsIgnoreCase("LastModifiedTimestamp"))
	        {
	        	Long time=Long.parseLong(pair.getValue().toString());
	        	Date dateObj = new Date(time*1000);
	        	System.out.println(pair.getKey() + " = " + dateObj);
	        }
	        else if(pair.getKey().toString().equalsIgnoreCase("ApproximateNumberOfMessages"))
	        {
	        	System.out.println(pair.getKey() + " = " + pair.getValue());
	        }
	    }
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    	catch(Exception e)
    	{
    		System.out.println("Error :" +e.getMessage());
    	}
		
	}
	
	//To Delete a Queue
	public static void deleteQueue()
	{
		try
		{
		amazonSQSClient.deleteQueue(deleteQueue);
		System.out.println("Queue "+ deleteQueue + " deleted Successfully ");
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    	catch(Exception e)
    	{
    		System.out.println("Error :" +e.getMessage());
    	}
	}
	
	//Send Messages from a Queue
	public static void sendMessage(String queue)
	{
		try
		{
		SendMessageRequest sendMessageRequest=new SendMessageRequest();
		sendMessageRequest.setQueueUrl(queue);
		ArrayList<String> messageList=new ArrayList<String>();
		messageList.add("Build your own dreams, or someone else will hire you to build theirs -- Farrah Gray");
		messageList.add("First they ignore you. Then they laugh at you. Then they fight you. Then you win -- Mahatma Gandhi");
		messageList.add("The mind is everything. What you think you become-- Buddha");
		messageList.add("The most difficult thing is the decision to act, the rest is merely tenacity-- Amelia Earhart");
		messageList.add("Too many of us are not living our dreams because we are living our fears -- Les Brown");
		SendMessageResult sendMessageResult =new SendMessageResult();
		
		for (String msg : messageList) {
			sendMessageRequest.setMessageBody(msg);
			sendMessageResult=amazonSQSClient.sendMessage(sendMessageRequest);
			System.out.println("Message Sent Successfully");
			System.out.println("Message ID :" + sendMessageResult.getMessageId());
		}
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    	catch(Exception e)
    	{
    		System.out.println("Error :" +e.getMessage());
    	}
		
	}
	
	//Receive and Delete Messages from a Queue
	public static void ReceiveMessage(String queue)
	{
		try
		{
		String receiptHandle="";
		ReceiveMessageRequest receiveMessageRequest=new ReceiveMessageRequest();
		receiveMessageRequest.setMaxNumberOfMessages(1);
		receiveMessageRequest.setQueueUrl(queue);
		ReceiveMessageResult receiveMessageResult=new ReceiveMessageResult();
		DeleteMessageRequest deleteMessageRequest=new DeleteMessageRequest();
		for (int i = 0; i < 3; i++) {
			receiveMessageRequest.setMaxNumberOfMessages(1);
			receiveMessageRequest.setQueueUrl(queue);
			receiveMessageResult=amazonSQSClient.receiveMessage(receiveMessageRequest);
			receiptHandle=receiveMessageResult.getMessages().get(0).getReceiptHandle();
			System.out.println(receiveMessageResult.getMessages().get(0).getBody());
			deleteMessageRequest.setQueueUrl(queue);
			deleteMessageRequest.setReceiptHandle(receiptHandle);
			amazonSQSClient.deleteMessage(deleteMessageRequest);
			System.out.println("Message deleted from Queue Successfully ");
			Thread.sleep(3000);
		}
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    	catch(Exception e)
    	{
    		System.out.println("Error :" +e.getMessage());
    	}
	}
	
	//Set Attributes for a Queue
	public static void setQueueAttributes(String queue)
	{
		try
		{
		SetQueueAttributesRequest setQueueAttributesRequest=new SetQueueAttributesRequest();
		setQueueAttributesRequest.addAttributesEntry("VisibilityTimeout", "3600");
		System.out.println("Visibility Timeout set to 1 hr(60 minutes)");
		setQueueAttributesRequest.addAttributesEntry("MaximumMessageSize", "131072");
		System.out.println("Maximum Message Size set to 128 KiB");
		setQueueAttributesRequest.addAttributesEntry("MessageRetentionPeriod", "172800");
		System.out.println("Message Retention Period set to 2 days");
		setQueueAttributesRequest.addAttributesEntry("ReceiveMessageWaitTimeSeconds", "15");
		System.out.println("Receive Message Wait Time set to 15 sec");
		setQueueAttributesRequest.setQueueUrl(queue);
		amazonSQSClient.setQueueAttributes(setQueueAttributesRequest);
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    	catch(Exception e)
    	{
    		System.out.println("Error :" +e.getMessage());
    	}
		
	}
	
	//To Purgue a Queue
	public static void purgueQueue(String queue)
	{
		try
		{
		PurgeQueueRequest purgeQueueRequest=new PurgeQueueRequest();
		purgeQueueRequest.setQueueUrl(queue);
		amazonSQSClient.purgeQueue(purgeQueueRequest);
		System.out.println("Purge Queue "+ queue + "Sucessfully ");
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    	catch(Exception e)
    	{
    		System.out.println("Error :" +e.getMessage());
    	}		
	}
	
	public static void main(String[] args) {
		

		String queue="";
    	try
    	{
    	int choice;
    	do
    	{
    		System.out.println("**** SQS is configured for the Region : US East ****");
    		System.out.println(" 1. Create  Queues \n 2. List Queue \n 3. Get Queue Attributes \n 4. Delete Queue \n "
    				+ "5. Send Message \n 6. Receive and Delete Messages \n 7. Set Queue Attributes \n 8. Purge Queue \n "
    				+" 9. Exit Program");
    		
    		System.out.println("Enter your choice(1- 9) : ");
    		Scanner input =new Scanner(System.in);
    		 choice=input.nextInt();
    		 
    		switch(choice){
    		
    		case 1: queue=createQueue();
    				break;
    		case 2: listQueue();
    				break;
    		case 3: getQueueAttributes(queue);
					break;
    		case 4: deleteQueue();
    				break;
    		case 5: sendMessage(queue);
					break;	
    		case 6: ReceiveMessage(queue);
					break;
    		case 7: setQueueAttributes(queue);
					break;	
    		case 8: purgueQueue(queue);
					break;
			default :System.out.println("Program Exited ");
					break;
    		
    		}
    		
    	}
    	while(choice<=8);
    	
    	}
    	catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    	catch(Exception e)
    	{
    		System.out.println("Error :" +e.getMessage());
    	}
    	
    
	}

}
