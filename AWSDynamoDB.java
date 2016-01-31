package edu.villanova.csc9010.rvempati;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

public class AWSDynamoDB {

	
	public static AWSCredentials credentials = new BasicAWSCredentials("xxxxxxxxx", "aaaaaaaaaaaaaaaa");
	
	public static AmazonDynamoDBClient amazonDynamoDBClient=new AmazonDynamoDBClient(credentials);
	
	public static int primaryKeyHash=40005;
	
	
	// create a table in DynamoDB
	public static void createTable()
	{
		try
		{
		List<KeySchemaElement> keyList=new ArrayList<KeySchemaElement>();
		KeySchemaElement keySchemaElement1=new KeySchemaElement();
		keySchemaElement1.withKeyType(KeyType.HASH);
		keySchemaElement1.withAttributeName("Number");
		keyList.add(keySchemaElement1);
		
		KeySchemaElement keySchemaElement2=new KeySchemaElement();
		keySchemaElement2.withKeyType(KeyType.RANGE);
		keySchemaElement2.withAttributeName("Gender");
		
		keyList.add(keySchemaElement2);
		
		List<AttributeDefinition> attributeList=new ArrayList<AttributeDefinition>();
		attributeList.add(new AttributeDefinition().withAttributeName("Number").withAttributeType(ScalarAttributeType.N));
		attributeList.add(new AttributeDefinition().withAttributeName("Gender").withAttributeType(ScalarAttributeType.S));
		
		
		ProvisionedThroughput provisionedThroughput=new ProvisionedThroughput();
		provisionedThroughput.setReadCapacityUnits(1L);
		provisionedThroughput.setWriteCapacityUnits(1L);
		
		
		CreateTableRequest createTableRequest=new CreateTableRequest();
		createTableRequest.setKeySchema(keyList);
		createTableRequest.setProvisionedThroughput(provisionedThroughput);
		createTableRequest.setAttributeDefinitions(attributeList);
		createTableRequest.setTableName("edu.villanova.csc9010.rvempati");
		amazonDynamoDBClient.createTable(createTableRequest);
		
		System.out.println("Table edu.villanova.csc9010.rvempati created Successfully ");
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
	
	//List all the tables in DynamoDB
	public static void listTables()
	{
		try
		{
		ListTablesResult listTablesResult=amazonDynamoDBClient.listTables();
		for (String table : listTablesResult.getTableNames()) {
			System.out.println("Table Name : " +table);
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
	//To Describe a Table
	public static void describeTable()
	{
		try
		{
		DescribeTableResult describeTableResult=amazonDynamoDBClient.describeTable("edu.villanova.csc9010.rvempati");
		
		TableDescription tableDescription= describeTableResult.getTable();
		System.out.println("Table Name : "+tableDescription.getTableName());
		System.out.println("Table Status : "+tableDescription.getTableStatus());
		System.out.println("Creation Date Time : "+tableDescription.getCreationDateTime());
		System.out.println("Item Count : "+tableDescription.getItemCount());
		System.out.println("Table Size Bytes : "+tableDescription.getTableSizeBytes());
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
	
	
	
	
	//To put a new record in DynamoDB
	
	public static void putNewItem()
	{
		try
		{
		DynamoDB dynamoDB=new DynamoDB(amazonDynamoDBClient);	
		Table table=dynamoDB.getTable("edu.villanova.csc9010.rvempati");
		Item item=new Item();
		Scanner input=new Scanner(System.in);
		System.out.println("Enter Gender : ");
		item.withPrimaryKey("Number", primaryKeyHash, "Gender", input.nextLine());
		System.out.println("Enter GivenName : ");
		item.withString("GivenName", input.nextLine());
		System.out.println("Enter MiddleInitial : ");
		item.withString("MiddleInitial", input.nextLine());
		System.out.println("Enter Surname : ");
		item.withString("Surname", input.nextLine());
		System.out.println("Enter EmailAddress : ");
		item.withString("EmailAddress", input.nextLine());
		System.out.println("Enter StreetAddress : ");
		item.withString("StreetAddress", input.nextLine());
		System.out.println("Enter City : ");
		item.withString("City", input.nextLine());
		System.out.println("Enter State : ");
		item.withString("State", input.nextLine());
		
		table.putItem(item);
		
		System.out.println("  Record Inserted Successfully ");
		primaryKeyHash++;
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
	
	//To fetch a record in DynamoDB Table
	public static void getItem()
	{
		try
		{
		DynamoDB dynamoDB = new DynamoDB(amazonDynamoDBClient);
		
		Scanner input=new Scanner(System.in);
		
		System.out.println("Enter Number :");
		int num=input.nextInt();
		
		System.out.println("Enter Gender :");
		String gender=input.next();
		
		
			Table table = dynamoDB.getTable("edu.villanova.csc9010.rvempati");

			Item item = table.getItem("Number", num, "Gender", gender);
			
			Iterator it = item.attributes().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        System.out.println(pair.getKey() + " = " + pair.getValue());
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
	
	//Update Table
	public static void updateTable() 
	{
		try
		{
		DynamoDB dynamoDB = new DynamoDB(amazonDynamoDBClient);

			Table table = dynamoDB.getTable("edu.villanova.csc9010.rvempati");

			ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
			    .withReadCapacityUnits(5L)
			    .withWriteCapacityUnits(5L);

			table.updateTable(provisionedThroughput);
			
			
			
			table.waitForActive();
			
			System.out.println("Table updated with Read and Write Throughput to 5 ");
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
	
	
	
	
	//To put Records in batches using BatchWriteItem
	public static void finalBatch()
	{

		try
		{
			URL oracle = new URL("https://s3.amazonaws.com/depasquale/datasets/personalData.txt");
			BufferedReader br = new BufferedReader(
			    new InputStreamReader(oracle.openStream())); 
			ArrayList<WriteRequest> listWriteRequest=new ArrayList<WriteRequest>();
			BatchWriteItemRequest batchWriteItemRequest=new BatchWriteItemRequest();
			
			String line=br.readLine();
			String[] tokens=line.split("\t");
			System.out.println("Records are inserting in Table..Please wait..");
			for(int j=0;j<1000;j++)
			{
				line=br.readLine();
			String[] data = line.split("\t");
			
			Map <String,AttributeValue> item= new HashMap<String, AttributeValue>();
			for (int i = 0; i < data.length; i++) 
			{
				AttributeValue attrVal = new AttributeValue();
				if(data[i]!=null)
				{
					if(i==0||i==8||i==14||i==15)
						attrVal.withN(data[i]);
					else	
						attrVal.withS(data[i]);
				}
				item.put(tokens[i], attrVal);
			}
			PutRequest putRequest = new PutRequest(item);
			WriteRequest writeRequest = new WriteRequest(putRequest);						
			listWriteRequest.add(writeRequest);
			if(listWriteRequest.size()==25)
			{						
				Map <String,java.util.List<WriteRequest>> requestItems= new HashMap<String,java.util.List<WriteRequest>>();
				requestItems.put("edu.villanova.csc9010.rvempati", listWriteRequest);
				batchWriteItemRequest.setRequestItems(requestItems);
				BatchWriteItemResult batchWriteItemResult = amazonDynamoDBClient.batchWriteItem(batchWriteItemRequest);
				listWriteRequest.clear();
				Map <String,java.util.List<WriteRequest>> unprocessedItems = batchWriteItemResult.getUnprocessedItems();
				
				for (Map.Entry<String,java.util.List<WriteRequest>> entry : unprocessedItems.entrySet()) {
					if(entry.getKey().equalsIgnoreCase("edu.villanova.csc9010.rvempati"))
					{
						
						listWriteRequest.addAll(entry.getValue());
					}
				}
			}
			}
			
			while(!listWriteRequest.isEmpty())
			{

				Map<String, java.util.List<WriteRequest>> requestItems = new HashMap<String, java.util.List<WriteRequest>>();
				requestItems.put("edu.villanova.csc9010.rvempati",
						listWriteRequest);
				batchWriteItemRequest.setRequestItems(requestItems);
				BatchWriteItemResult batchWriteItemResult = amazonDynamoDBClient
						.batchWriteItem(batchWriteItemRequest);
				listWriteRequest.clear();
				Map<String, java.util.List<WriteRequest>> unprocessedItems = batchWriteItemResult
						.getUnprocessedItems();

				for (Map.Entry<String, java.util.List<WriteRequest>> entry : unprocessedItems
						.entrySet()) {
					if (entry.getKey().equalsIgnoreCase(
							"edu.villanova.csc9010.rvempati")) {

						listWriteRequest.addAll(entry.getValue());
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
    		System.out.println("Error :" +e.getMessage());
    	}
		
		
		
	}
	
	//To update a Item
	public static void updateItem()
	{
		try
		{
		Scanner input=new Scanner(System.in);
		System.out.println("Enter Row Number : ");
		int row=input.nextInt();
		DynamoDB dynamoDB=new DynamoDB(amazonDynamoDBClient);
		Table table=dynamoDB.getTable("edu.villanova.csc9010.rvempati");
		
		List<Map<String, AttributeValue>> item=scan("edu.villanova.csc9010.rvempati", row);
		
		Item resitem = table.getItem("Number", row, "Gender", item.get(0).get("Gender").getS());
		
		System.out.println("First Name : "+ resitem.getString("GivenName"));
		System.out.println("Last Name : "+ resitem.getString("Surname"));
		
		System.out.println("Enter new First Name : ");
		String first=input.next();
		System.out.println("Enter new Last Name : ");
		String last=input.next();
		
		Map<String, String> expressionAttributeNames = new HashMap<String, String>();
		expressionAttributeNames.put("#G", "GivenName");
		expressionAttributeNames.put("#S", "Surname");
		
		Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
		expressionAttributeValues.put(":val1", first);
		expressionAttributeValues.put(":val2", last);
		
		table.updateItem("Number", resitem.get("Number"), "Gender", resitem.get("Gender"), "SET #G=:val1,#S=:val2",expressionAttributeNames,expressionAttributeValues);
		
		System.out.println("Record Updated Successfully ");

		System.out.println("After Updation ");
		System.out.println("First Name : "+table.getItem("Number", row, "Gender", item.get(0).get("Gender").getS()).getString("GivenName"));
		System.out.println("Last Name : "+table.getItem("Number", row, "Gender", item.get(0).get("Gender").getS()).getString("Surname"));
		
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
	//Delete a Item
	public static void deleteItem()
	{
		try
		{
		Scanner input=new Scanner(System.in);
		DynamoDB dynamoDB=new DynamoDB(amazonDynamoDBClient);
		Table table=dynamoDB.getTable("edu.villanova.csc9010.rvempati");
		
		System.out.println("Enter Row Number : ");
		int row=input.nextInt();
		
		List<Map<String, AttributeValue>> item=scan("edu.villanova.csc9010.rvempati", row);
		
		System.out.println("gender "+item.get(0).get("Gender").getS());
		
		table.deleteItem("Number", row, "Gender", item.get(0).get("Gender").getS());
		
		
		System.out.println("Record Deleted Successfully ");
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
	
	
	
	//Get records in a Table
	public static void query()
	{
		DynamoDB dynamoDB=new DynamoDB(amazonDynamoDBClient);
		Table table=dynamoDB.getTable("edu.villanova.csc9010.rvempati");
		QuerySpec querySpec=new QuerySpec();
		Scanner input=new Scanner(System.in);
		System.out.println("Enter Lower Range for Row Number : ");
		int lower=Integer.parseInt(input.nextLine());
		
		System.out.println("Enter Upper Range for Row Number : ");
		int upper=Integer.parseInt(input.nextLine());
		
		for(int i=lower;i<=upper;i++)
		{
		querySpec.withKeyConditionExpression("#N =:start")
				.withNameMap(new NameMap().with("#N", "Number"))
				.withValueMap(new ValueMap()
                .withNumber(":start", i));
		
		 ItemCollection<QueryOutcome> outcome=table.query(querySpec);
		 
	            Iterator<Item> list=outcome.iterator();
	            while (list.hasNext()) {
		            Iterator list2=list.next().attributes().iterator();
		            while(list2.hasNext())
		            {
		            	Map.Entry pair = (Map.Entry)list2.next();
				        System.out.println(pair.getKey() + " = " + pair.getValue());
		            }
		            System.out.println("==========================");
		        }
		}
		
	}
//Delete a Table
	public static void deleteTable()
	{
		try
		{
		amazonDynamoDBClient.deleteTable("edu.villanova.csc9010.rvempati");
		System.out.println("Table edu.villanova.csc9010.rvempati Deleted Successfully ");
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
	
	
	//get a record from a table by providing the row number
	public static List<Map<String, AttributeValue>> scan(String tableName, int rowNo)
	{
		
		
		List<Map<String, AttributeValue>> result=null;
		HashMap<String, Condition> conditionFilter = new HashMap<String,Condition>();
		
	       Condition condition = new Condition()
	            .withComparisonOperator(ComparisonOperator.EQ.toString())
	            .withAttributeValueList(new AttributeValue().withN(""+rowNo+""));
	       
	       
	       conditionFilter.put("Number", condition);
	        
	       System.out.println("Requesting the Item...\n");
	       
	       ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(conditionFilter);
	        
	        ScanResult scanResult = amazonDynamoDBClient.scan(scanRequest);
	        
	        if(scanResult.getCount()==0){
	        	System.out.println("There are no Items in "+ tableName +" with the given row number");
	        
	        }
	        else{
	        	result=scanResult.getItems();
	     	        }
		
	        return result;
	}
	
	
	
	public static void main(String[] args) {

		try
    	{
    	int choice;
    	do
    	{
    		System.out.println("**** Dynamo DB is configured for the Region : US East ****");
    		System.out.println(" 1. Create  Table \n 2. List Tables \n 3. Describe Table \n 4. Put Item \n "
    				+ "5. Get Item \n 6. Update Table \n 7. Batch Write Item \n "
    				+" 8. Update Item \n 9.Query \n 10.Delete Item \n 11. Delete Table \n 12. Program Exited");
    		
    		System.out.println("Enter your choice(1- 13) : ");
    		Scanner input =new Scanner(System.in);
    		 choice=input.nextInt();
    		 
    		switch(choice){
    		
    		case 1: createTable();
    				break;
    		case 2: listTables();
    				break;
    		case 3: describeTable();
					break;
    		case 4: putNewItem();
    				break;
    		case 5: getItem();
					break;	
    		case 6: updateTable();
					break;
    		case 7: finalBatch();
					break;	
    		case 8: updateItem();
    				break;
    		case 9: query();
    				break;
    		case 10: deleteItem();
    				break;
    		case 11: deleteTable();
    				break;		
			default :System.out.println("Program Exited ");
					break;
    		
    		}
    		
    	}
    	while(choice<=11);
    	
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
