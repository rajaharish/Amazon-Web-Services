package Cloud.Project2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeRegionsResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Placement;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.DeleteVerifiedEmailAddressRequest;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.GetSendQuotaResult;
import com.amazonaws.services.simpleemail.model.ListVerifiedEmailAddressesResult;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import com.amazonaws.services.simpleemail.model.SendEmailResult;
import com.amazonaws.services.simpleemail.model.VerifyEmailAddressRequest;

public class Amazon_EC2 {
	
	
	
	


    

	public static AWSCredentials credentials = new BasicAWSCredentials("xxxxxxxxxxxxx", "aaaaaaaaaaaaaaaaaaa");    

	public static AmazonSimpleEmailService amazonSimpleEmailServiceClient=new AmazonSimpleEmailServiceClient(credentials);
	
	public static AmazonEC2Client amazonEC2Client=new AmazonEC2Client(credentials);
	
	public static Region uswestto =Region.getRegion(Regions.US_WEST_2);
	
	public static String instanceId="";
	
	
	//To Run a EC2 Instance
	
	public static String createEC2Instance()
	{
		String selectedRegion="";
		try
		{
		
		System.out.println("=====List of Regions==== ");
		
		DescribeRegionsResult describeRegionsResult=amazonEC2Client.describeRegions();
		
		List<com.amazonaws.services.ec2.model.Region> region= describeRegionsResult.getRegions();
		
		List<String> strRegion=new ArrayList<String>();
		
		
		for (int i = 1; i <=region.size(); i++) {
			System.out.println(i +". "+region.get(i-1).getRegionName());
			strRegion.add(region.get(i-1).getRegionName());
		}
		
 		
		System.out.println("Type a number to select a region");
		Scanner input =new Scanner(System.in);
		int givenRegion=input.nextInt();
		
		amazonEC2Client.setRegion(Region.getRegion(Regions.valueOf(strRegion.get(givenRegion-1).toUpperCase().replace('-', '_'))));
		
		selectedRegion=strRegion.get(givenRegion-1).toUpperCase().replace('-', '_');
		
		
		DescribeAvailabilityZonesResult describeAvailabilityZonesResult=amazonEC2Client.describeAvailabilityZones();
		
		List<AvailabilityZone> availabilityZones=describeAvailabilityZonesResult.getAvailabilityZones();
		
		System.out.println("=======List of Zones=======");
		
		for (AvailabilityZone availabilityZone : availabilityZones) {
			System.out.println(availabilityZone.getZoneName());
		}
		
		System.out.println("To Select a Zone please provide the exact name as appeared : ");
		
		String zone=input.next();
		 Placement place = new Placement();
        place.setAvailabilityZone(zone);
        
        RunInstancesRequest runInstancesRequest=new RunInstancesRequest();
        
        switch(givenRegion){
        case 1:
			runInstancesRequest.withImageId("ami-c887bed5").withKeyName("rvempati_eu_central1").withInstanceType("t2.micro");
			break;
		case 2:
			runInstancesRequest.withImageId("ami-09e25b14").withKeyName("rvempati_sa_east1").withInstanceType("t2.micro");
			break;
		case 3:
			runInstancesRequest.withImageId("ami-cbf90ecb").withKeyName("rvempati_ap_northeast1").withInstanceType("t2.micro");
			break;
		case 4:
			runInstancesRequest.withImageId("ami-a10897d6").withKeyName("rvempati_eu_west1").withInstanceType("t2.micro");
			break;
		case 5:
			runInstancesRequest.withImageId("ami-12663b7a").withKeyName("rvempati_us_east1").withInstanceType("t2.micro");
			break;
		case 6:
			runInstancesRequest.withImageId("ami-0d6d8749").withKeyName("rvempati_us_west1").withInstanceType("t2.micro");
			break;
		case 7:
			runInstancesRequest.withImageId("ami-6989a659").withKeyName("rvempati_us_west2").withInstanceType("t1.micro");
			break;
		case 8:
			runInstancesRequest.withImageId("ami-fd9cecc7").withKeyName("rvempati_ap_southeast2").withInstanceType("t2.micro");
			break;
		case 9:
			runInstancesRequest.withImageId("ami-dc1c2b8e").withKeyName("rvempati_ap_southeast1").withInstanceType("t2.micro");
			break;
		default:System.out.println("Invalid choice");
			break;
        		
        }
        
        runInstancesRequest.withMinCount(1).withMaxCount(1);
        RunInstancesResult runInstancesResult= amazonEC2Client.runInstances(runInstancesRequest);
        
       instanceId= runInstancesResult.getReservation().getInstances().get(0).getInstanceId();
       
       System.out.println("Creating the Instance...Please wait..");
       
       Thread.sleep(20000);
       
       System.out.println("*** Instance Created Successfully *** ");
       
       System.out.println("Instance Id of the newly created EC2 Instance : " + instanceId);
       
       for(Reservation reservation:amazonEC2Client.describeInstances().getReservations())
		{
			for(Instance instance:reservation.getInstances())
			{
				if(instance.getInstanceId().equalsIgnoreCase(instanceId))
				System.out.println("Public IP Address of the newly created EC2 Instance : " +instance.getPublicIpAddress());
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
    		e.printStackTrace();
    	}
       return selectedRegion;
        
	}
	
	//To describe EC2 Instance 
	public static void describeInstances(String Reg)
	{
		try
		{
		amazonEC2Client.setRegion(Region.getRegion(Regions.valueOf(Reg)));
		
		System.out.println("===Instance ID's of the Available Instances in the Region : "+ Reg +"   ====");
		
		for(Reservation reservation:amazonEC2Client.describeInstances().getReservations())
		{
			for(Instance instance:reservation.getInstances())
			{
				System.out.println(instance.getInstanceId());
			}
		}
		
		
		System.out.println("To get the Details, select an Instance fron the Above list by providing the exact Instance ID :  ");
		
		Scanner input=new Scanner(System.in);
		String selectedInstance=input.next();
		
		
		for(Reservation reservation:amazonEC2Client.describeInstances().getReservations())
		{
			for(Instance instance:reservation.getInstances())
			{
				if(selectedInstance.equalsIgnoreCase(instance.getInstanceId()))
				{
					System.out.println(instance.toString());
					
					
				}
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
    		e.printStackTrace();
    	}
		
			
	}
	
	//To Terminate an EC2 Instance
	
	public static void terminateInstance(String Reg)
	{
		try
		{
		amazonEC2Client.setRegion(Region.getRegion(Regions.valueOf(Reg)));
		
		System.out.println("===Instance ID's of the Available Instances in the Region : "+ Reg +"   ====");
		
		
		for(Reservation reservation:amazonEC2Client.describeInstances().getReservations())
		{
			for(Instance instance:reservation.getInstances())
			{
				System.out.println(instance.getInstanceId());
			}
		}
		
		
		System.out.println("To Terminate, select an Instance fron the Above list by providing the exact Instance ID :  ");
		
		Scanner input=new Scanner(System.in);
		String selectedInstance=input.next();
		
		
		amazonEC2Client.terminateInstances(new TerminateInstancesRequest().withInstanceIds(selectedInstance));
		
		System.out.println("Terminating Instance...Please wait.. ");
		
		Thread.sleep(10000);
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
    		e.printStackTrace();
    	}
	}
	
	//To display list of verified Emails for a Account
	
	public static void listVerifiedEmails()
	{
		try
		{
			
			amazonSimpleEmailServiceClient.setRegion(uswestto);
			amazonSimpleEmailServiceClient.setEndpoint("email.us-west-2.amazonaws.com");
			
			System.out.println("=== SES configured for the Region  : " + uswestto + " === ");
			
		ListVerifiedEmailAddressesResult listVerifiedEmailAddressesResult=amazonSimpleEmailServiceClient.listVerifiedEmailAddresses();
		
		List<String> VerifiedEmailList=listVerifiedEmailAddressesResult.getVerifiedEmailAddresses();
		
		System.out.println("=== List of Verified Emails for the Region : "+ uswestto+ " ===");
		for(String eachMail: VerifiedEmailList)
		{
			System.out.println(eachMail);
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
    		e.printStackTrace();
    	}
		
		 
		
	}
	
	//To display Email statistics for the account
	public static void getEmailQuotaAndStatistics()
	{
		
		amazonSimpleEmailServiceClient.setRegion(uswestto);
		amazonSimpleEmailServiceClient.setEndpoint("email.us-west-2.amazonaws.com");
		
		System.out.println("=== SES configured for the Region  : "+uswestto + " ===");
	
		
		System.out.println("Send Quota Details : " +amazonSimpleEmailServiceClient.getSendQuota().toString());
        
        System.out.println("Send Statistics Details : "+ amazonSimpleEmailServiceClient.getSendStatistics().toString());
		
		
	}
	
	
	//To send a Simple Email
	
	public static void sendEmail()
	{
		try
		{
		amazonSimpleEmailServiceClient.setRegion(uswestto);
		amazonSimpleEmailServiceClient.setEndpoint("email.us-west-2.amazonaws.com");
		
		System.out.println("=== SES configured for the Region  : "+uswestto + " ===");
		
		System.out.println("Enter the Destination Email Address :  ");
		
		Scanner input=new Scanner(System.in);
		
		String destinationEmail=input.next();
		
		String subject="Hello from SES";
		String body="Greetings " +destinationEmail+"  from Amazon SES!";
		
		List<String> destinationList=new ArrayList<String>();
		
		destinationList.add(destinationEmail);
		
		Content subjectContent = new Content(subject);
        Content bodyContent = new Content(body);
        Body msgBody = new Body(bodyContent);
        Message msg = new Message(subjectContent, msgBody);
        Destination destination=new Destination(destinationList);

        SendEmailRequest request = new SendEmailRequest("rvempati@villanova.edu", destination, msg);

        SendEmailResult result = amazonSimpleEmailServiceClient.sendEmail(request);

        System.out.println("*** Email sent successfully ***");  
        
        
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
    		e.printStackTrace();
    	}
	}
	
	//To Send an Email including CC and BCC
	
	public static void sendEmailWithCCAndBCC()
	{
		
		try
		{
		amazonSimpleEmailServiceClient.setRegion(uswestto);
		amazonSimpleEmailServiceClient.setEndpoint("email.us-west-2.amazonaws.com");
		
		System.out.println("=== SES configured for the Region  : "+uswestto+ " ===");
		
		System.out.println("Enter the Destination Email Address : ");
		SendEmailRequest request=new SendEmailRequest();
		
		Scanner input=new Scanner(System.in);
		
		String destinationEmail=input.next();
		
		List<String> destinationList=new ArrayList<String>();
		List<String> ccList=new ArrayList<String>();
		List<String> bccList=new ArrayList<String>();
		
		destinationList.add(destinationEmail);
		
		System.out.println("Enter Number of Emails to include as CC");
		int ccCount=input.nextInt();
		String ccEmail="";
		for(int i=1;i<=ccCount;i++)
		{
			System.out.println("Enter Email Address "+i+" : ");
			ccEmail=input.next();
			ccList.add(ccEmail);
		}
		
		System.out.println("Enter Number of Emails to include as BCC");
		
		int bccCount=input.nextInt();
		String bccEmail="";
		for(int i=1;i<=bccCount;i++)
		{
			System.out.println("Enter Email Address "+i+" : ");
			bccEmail=input.next();
			bccList.add(bccEmail);
		}
		
		String subject="Hello from SES";
		String body="Greetings " +destinationEmail+"  from Amazon SES!";
		Content subjectContent = new Content(subject);
        Content bodyContent = new Content(body);
        Body msgBody = new Body(bodyContent);
        Message msg = new Message(subjectContent, msgBody);
		
		Destination destination=new Destination();
		destination.setToAddresses(destinationList);
		destination.setCcAddresses(ccList);
		destination.setBccAddresses(bccList);
		
		  SendEmailRequest emailRequest = new SendEmailRequest("rvempati@villanova.edu", destination, msg);

	        SendEmailResult result = amazonSimpleEmailServiceClient.sendEmail(emailRequest);
	        
	        System.out.println("*** Email Sent Successfully ***");
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
    		e.printStackTrace();
    	}
		
	}
	
	//To delete a Verified Email Address from the Account
	
	public static void deleteVerifiedEmailAddress()
	{
		try
		{
		listVerifiedEmails();
		System.out.println("To delete, Select a Email fron Above List by providing the complete Email Address : ");
		Scanner input=new Scanner(System.in);
		String email=input.next();
		 amazonSimpleEmailServiceClient.deleteVerifiedEmailAddress(new DeleteVerifiedEmailAddressRequest().withEmailAddress(email));
		 
		 System.out.println("*** Email ID : "+ email + " deleted Successfully ***");
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
    		e.printStackTrace();
    	}
	}
	
	
	//To verify a Email Address 
	
	public static void verifyEmailAddress()
	{
		try
		{
		amazonSimpleEmailServiceClient.setRegion(uswestto);
		amazonSimpleEmailServiceClient.setEndpoint("email.us-west-2.amazonaws.com");
		
		System.out.println("=== SES configured for the Region  : "+uswestto+ " ===");
		
		System.out.println("Enter the Email Address to be verified : ");
		
		Scanner input=new Scanner(System.in);
		
		String email=input.next();
		
		amazonSimpleEmailServiceClient.verifyEmailAddress(new VerifyEmailAddressRequest().withEmailAddress(email));
		
		System.out.println("*** Verification Email sent Successfully *** ");
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
    		e.printStackTrace();
    	}
		
	}
	
	public static void main(String[] args) {
		
		
		String reg="";
		try
    	{
			
    	int choice;
    	do
    	{
    		System.out.println("====== Menu ======");
    		System.out.println(" 1. List Verified Emails \n 2. Send Email \n 3. Get Email Quota and Statistics \n 4. Send Email with  CC and BCC \n 5. Verify Email Address  \n 6. Delete Verified Email Address  \n 7. Create Instance \n "
    				+ "8. Describe Instance \n 9. Terminate Instance \n 10. Exit Program");
    		
    		System.out.println("Enter your choice(1- 9) : ");
    		Scanner input =new Scanner(System.in);
    		 choice=input.nextInt();
    		
    		 
    		switch(choice){
    		
    		case 1: listVerifiedEmails();
    				break;
    		case 2: sendEmail();
    				break;
    		case 3: getEmailQuotaAndStatistics();
    				break;
    		case 4: sendEmailWithCCAndBCC();
    				break;
    		case 5: verifyEmailAddress();
					break;
    		case 6: deleteVerifiedEmailAddress();
					break;	
    		case 7: reg=createEC2Instance();
					break;
    		case 8: describeInstances(reg);
					break;	
    		case 9:terminateInstance(reg);
					break;
			default :System.out.println("Program Exited ");
					break;
    		
    		}
	   }
    	while(choice<=9);
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
    		e.printStackTrace();
    	}

	}
}
