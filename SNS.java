package Cloud.Project2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.ListSubscriptionsResult;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sns.model.UnsubscribeRequest;

public class SNS {

	
	public static AWSCredentials credentials = new BasicAWSCredentials("xxxxxxxxxxxx", "aaaaaaaaaaaaaaa");
	
	public static AmazonSNSClient amazonSNSClient=new AmazonSNSClient(credentials);
	
	public static String mobileSubscriptionARN="";
	
	public static String emailSubscriptionARN="";
	
	public static String topic1="";
	
	public static String topic2="";
	
	
	//creates two Topics
	public static void createTopic()
	{
		try
		{
		CreateTopicResult createTopicResult1=amazonSNSClient.createTopic("CSC470Test-Alpha");
		
		topic1=createTopicResult1.getTopicArn();
		
		System.out.println("Topic CSC470Test-Alpha created Successfully");
		
		CreateTopicResult createTopicResult2=amazonSNSClient.createTopic("CSC470Test-Beta");
		
		topic2=createTopicResult2.getTopicArn();
		
		System.out.println("Topic CSC470Test-Beta created Successfully");
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
	
	//List the Available Topics
	public static void listTopics()
	{
		try
		{
		ListTopicsResult listTopicsResult=amazonSNSClient.listTopics();
		System.out.println("  List of Topics : ");
		for (Topic topic : listTopicsResult.getTopics()) {
			System.out.println(topic.getTopicArn());
			
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
	
	//Delete the Topic
	public static void deleteTopic(String topic)
	{
		try
		{
		amazonSNSClient.deleteTopic(topic);
		
		System.out.println("Topic CSC470Test-Beta Deleted Successfully ");
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
	
	//Get the Topic Attributes
	public static void getTopicAttributes()
	{
		try
		{
		GetTopicAttributesResult getTopicAttributesResult=amazonSNSClient.getTopicAttributes(topic1);
		
		Map<String, String> attributes=getTopicAttributesResult.getAttributes();
		Iterator it = attributes.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        if(!pair.getKey().toString().equalsIgnoreCase("EffectiveDeliveryPolicy"))
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
	
	
	//Set Topic Attributes
	public static void setTopicAttributes()
	{
		try
		{
		amazonSNSClient.setTopicAttributes(topic1, "DisplayName", "CSC470Testing");
		
		System.out.println("Display Name changed to CSC470Testing for Topic CSC470Test-Alpha  Successfully ");
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
	
	//Subscribe to a Topic
	public static void subscribe()
	{
		try
		{
		SubscribeRequest subscribeRequest=new SubscribeRequest();
		subscribeRequest.setTopicArn(topic1);
		subscribeRequest.setProtocol("Email-JSON");
		Scanner input=new Scanner(System.in);
		System.out.println("Enter Email Address ");
		String email=input.next();
		subscribeRequest.setEndpoint(email);
		SubscribeResult subscribeResult1=amazonSNSClient.subscribe(subscribeRequest);
		emailSubscriptionARN=subscribeResult1.getSubscriptionArn();
		
		System.out.println("Email Subscription created Successfully.. Pending Confirmation");
		
		subscribeRequest.setTopicArn(topic1);
		subscribeRequest.setProtocol("SMS");
		System.out.println("Enter Phone Number in the format(1-xxx-xxx-xxxx)");
		String phone=input.next();
		subscribeRequest.setEndpoint(phone);
		SubscribeResult subscribeResult=amazonSNSClient.subscribe(subscribeRequest);
		mobileSubscriptionARN=subscribeResult.getSubscriptionArn();
		
		System.out.println("Mobile Subscription created Successfully..Pending Confirmation ");
		
		System.out.println("Please confirm the Subscriptions before Unsubscribing");
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
	
	//List the Available Subscriptions
	public static void listSubscription()
	{
		try
		{
		ListSubscriptionsResult listSubscriptionsResult= amazonSNSClient.listSubscriptions();
		
		List<Subscription> subscriptionlist=listSubscriptionsResult.getSubscriptions();
		
		for (Subscription subscription : subscriptionlist) {
			System.out.println("SubscriptionARN  : "+ subscription.getSubscriptionArn());
			System.out.println("TopicARN : "+subscription.getTopicArn());
			System.out.println("Protocol : "+subscription.getProtocol());
			System.out.println("Owner : "+ subscription.getOwner());
			System.out.println("Endpoint : " +subscription.getEndpoint());
			System.out.println("==================================");
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
	
	//Publish a Message
	public static void publish()
	{
		try
		{
		PublishResult publishResult=amazonSNSClient.publish(topic1, "Test Message");
		
		System.out.println("Message Published Successfully with Request ID : "+publishResult.getMessageId());
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
	
	//Unsubscribe to a Topic
	public static void unsubscribe()
	{
		try
		{
		ListSubscriptionsResult listSubscriptionsResult= amazonSNSClient.listSubscriptions();
		
		List<Subscription> subscriptionlist=listSubscriptionsResult.getSubscriptions();
		
		for (Subscription subscription : subscriptionlist) {
			if(subscription.getProtocol().equalsIgnoreCase("SMS"))
			{
				
				String arn=subscription.getTopicArn();
				amazonSNSClient.unsubscribe(arn);
				System.out.println("Unsubscribed Successfully");
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
	
	//Delte Email Sunbscription
	public static void deleteEmail()
	{
		
		ListSubscriptionsResult listSubscriptionsResult= amazonSNSClient.listSubscriptions();
		
		List<Subscription> subscriptionlist=listSubscriptionsResult.getSubscriptions();
		
		for (Subscription subscription : subscriptionlist) {
			if(subscription.getProtocol().equalsIgnoreCase("Email-JSON"))
			{
				
				String arn=subscription.getTopicArn();
				amazonSNSClient.unsubscribe(arn);
				System.out.println("Unsubscribed Successfully");
			}
		}
	}
	
	public static void main(String[] args) {
		
		try
    	{
    	int choice;
    	do
    	{
    		System.out.println("**** SNS is configured for the Region : US East ****");
    		System.out.println(" 1. Create  Topic \n 2. List Topics \n 3. Delete Topic \n 4. Get Topic Attributes \n "
    				+ "5. Set Topic Attributes \n 6. Subscribe \n 7. List Subscription \n 8. Publish \n "
    				+" 9. Unsubscribe \n 10. Exit Program");
    		
    		System.out.println("Enter your choice(1- 10) : ");
    		Scanner input =new Scanner(System.in);
    		 choice=input.nextInt();
    		 
    		switch(choice){
    		
    		case 1: createTopic();
    				break;
    		case 2: listTopics();
    				break;
    		case 3: deleteTopic(topic2);
					break;
    		case 4: getTopicAttributes();
    				break;
    		case 5: setTopicAttributes();
    				getTopicAttributes();
					break;	
    		case 6: subscribe();
					break;
    		case 7: listSubscription();
					break;	
    		case 8: publish();
					break;
    		case 9: unsubscribe();
    				break;
			default :System.out.println("Program Exited ");
					break;
    		
    		}
    		
    	}
    	while(choice<=9);
    	
    	deleteTopic(topic1);
    	deleteEmail();
    	
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
