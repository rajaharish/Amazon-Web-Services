
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3ObjectSummary;


 
public class AwsS3 {
    
    public static String version="";
    
    public static AWSCredentials credentials = new BasicAWSCredentials("xxxxxxxxxxxxxx", "aaaaaaaaaaaaa");
    
    public static AmazonS3 s3client = new AmazonS3Client(credentials);
    
    //put a new object in a bucket
    public static void putObject(String bucket)
    {
        s3client.putObject(new PutObjectRequest(bucket, "testfile", 
            new File("/Users/harish/Documents/cloudcomputing/slides/assignment2/test.txt")));
        
        System.out.println("Object with key 'test' is placed in bucket "+bucket + "successfully");
    }
    
    //to delete a object in a bucket
    public static void deleteObject(String bucket,String key)
    {
        s3client.deleteObject(bucket, key);
    }
    
    //to create a new bucket
    public static void createBucket()
    {
        s3client.createBucket("villanova-csc9010-rvempati-testbucket");
    }
    
    //to delete a bucket
    public static void deleteBucket()
    {
        s3client.deleteBucket("villanova-csc9010-rvempati-testbucket");
    }
    
    //method to check whether a bucket exists or not and its permissions
    public static void headBucket(String bucket)
    {
        boolean isBucketExist=s3client.doesBucketExist(bucket);
        if(isBucketExist)
        {
            System.out.println("Bucket : "+bucket+ " exist ");
        }
        else
        {
            System.out.println("Bucket : "+ bucket+ "  doesn't exist");
        }
        
        AccessControlList accesslist=s3client.getBucketAcl(bucket);
        
        for(Grant g : accesslist.getGrantsAsList())
        {
            System.out.println("Permission : " +g.getPermission().toString());
        }
    }
    
    //method to copy object from one bucket to other
    public static void copyObject(String sourceBucket,String sourceKey)
    {
        s3client.copyObject(sourceBucket, sourceKey, "villanova-csc9010-rvempati-testbucket", "copiedobject");
    }
    
    //to get metadata of an object
    public static void headObject(String bucketName,String key)
    {
        ObjectMetadata objectMetadata=s3client.getObjectMetadata(bucketName, key);
        System.out.println(objectMetadata.toString());
    }
    
    //enable versioning for bucket
    public static void putBucketVersioning(String bucketName) {
        BucketVersioningConfiguration configuration = 
                new BucketVersioningConfiguration().withStatus("Enabled");
        
        SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest = 
                new SetBucketVersioningConfigurationRequest(bucketName,configuration);
        
        s3client.setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest);
        
    }
    
    
    //put one or more versions of an object
    public static void putMultipleVersionObjects(String bucket)
    {
        PutObjectResult putObjectResult= s3client.putObject(new PutObjectRequest(bucket, "versioncheck", 
                new File("/Users/harish/Documents/cloudcomputing/slides/assignment2/MultiVersion.txt")));
        
         version = putObjectResult.getVersionId();
        
        System.out.println("version ID of first put operation: "+ version);
        
        PutObjectResult putObjectResult2= s3client.putObject(new PutObjectRequest(bucket, "versioncheck", 
                new File("/Users/harish/Documents/cloudcomputing/slides/assignment2/MultiVersion.txt")));
        
        version = putObjectResult2.getVersionId();
        
        System.out.println("version id of second put operation : "+ version);
        
        
    }
    
    
    //to delete specific version of an object
    public static void deleteSpecificVersionObject(String bucketName)
    {
        s3client.deleteVersion(bucketName, "versioncheck", "PcDCkBsDrGlJqJyXe8kUJVmrqVNkbxu.");
    }
    
    //to get meta data of an object
    public static void objectMetadata(String bucket,String key)
    {
        //ListObjectsRequest listobjectrequest=new ListObjectsRequest().withBucketName(bucket);
        //ObjectListing list= s3client.listObjects(listobjectrequest);
        
        GetObjectMetadataRequest getObjectMetadataRequest=new GetObjectMetadataRequest(bucket, key);
        
        ObjectMetadata objectMetadata=s3client.getObjectMetadata(getObjectMetadataRequest);
        
       Map<String,Object> metadata= objectMetadata.getRawMetadata();
       
       Iterator it = metadata.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());
           
        }
        
    /*  for(S3ObjectSummary summary: list.getObjectSummaries())
        {
            System.out.println("Storage class : "+summary.getStorageClass());
            System.out.println("Size  : " +summary.getSize());
            System.out.println(" Last Modified  : "+  summary.getLastModified());
            System.out.println("ETag :  " + summary.getETag());
        }*/
    }
    
    
    //to set expiration time and storage class
    public static void putObjectWithExpiration(String bucket)
    {
        try
        {
        Calendar now = Calendar.getInstance();
        now.add(Calendar.DATE,5);
        Date date = now.getTime();
        System.out.println("date is :" + date);
        PutObjectRequest putObjectRequest=new PutObjectRequest(bucket, "expirycheck",new File("/Users/harish/Documents/cloudcomputing/slides/assignment2/test.txt")).withStorageClass(StorageClass.ReducedRedundancy);
    /*  PutObjectResult putObjectResult=s3client.putObject(putObjectRequest);
        putObjectResult.setExpirationTime(date);*/
        
        s3client.putObject(putObjectRequest);
        
        BucketLifecycleConfiguration con = s3client.getBucketLifecycleConfiguration(bucket);
        BucketLifecycleConfiguration configuration ;
        if(con == null) {
            
            

            BucketLifecycleConfiguration.Rule ruleArchiveAndExpire = new BucketLifecycleConfiguration.Rule()
                .withId("Archive and delete rule")
                .withPrefix("expirycheck")
                .withExpirationInDays(5)
                .withStatus(BucketLifecycleConfiguration.ENABLED.toString());

            java.util.List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<BucketLifecycleConfiguration.Rule>();
            rules.add(ruleArchiveAndExpire);

             configuration = new BucketLifecycleConfiguration()
               .withRules(rules);

            
        s3client.setBucketLifecycleConfiguration(bucket, configuration);
        
        }
        else
        {
         configuration = s3client.getBucketLifecycleConfiguration(bucket);

            
            configuration.getRules().add(
                    new BucketLifecycleConfiguration.Rule()
                        .withId("NewRule")
                        .withPrefix("expirycheck")
                        .withExpirationInDays(5)
                        .withStatus(BucketLifecycleConfiguration.
                            ENABLED.toString())
                       );
            // Save configuration.
            s3client.setBucketLifecycleConfiguration(bucket, configuration);
        }
            System.out.println("Object "+ "  "+ "expirycheck "+" succesfully uploaded with expiration 5 days");
        
        
        
        
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
    
    
    
    // to get an object from a bucket
    public static void getObject(String bucket,String key) 
    {
        
        try
        {
        S3Object obj=s3client.getObject(bucket, key);
        
        InputStream reader = new BufferedInputStream(
                   obj.getObjectContent());
                File file = new File("./downloadedfile");      
                OutputStream writer = new BufferedOutputStream(new FileOutputStream(file));

                int read = -1;

                while ( ( read = reader.read() ) != -1 ) {
                    writer.write(read);
                }

                writer.flush();
                writer.close();
                reader.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        
    }
    
    
    public static void main( String[] args )
    {
        
        try
        {
            String bucketName="";
        for (Bucket b : s3client.listBuckets()) {
            bucketName=b.getName();
            System.out.println(" Bucket Name:  " + bucketName);
        }
        //putObject(bucketName);
        //deleteObject(bucketName, "testfile");
        //getObject(bucketName,"testfile");
        //createBucket();
        //deleteBucket();
        //headBucket("villanova-csc9010-rvempati-testbucket");
        //copyObject("villanova-csc9010-rvempati", "testfile");
        //headObject("villanova-csc9010-rvempati", "testfile");
        //putBucketVersioning("villanova-csc9010-rvempati-testbucket");
        //putMultipleVersionObjects("villanova-csc9010-rvempati-testbucket");
        //deleteSpecificVersionObject("villanova-csc9010-rvempati-testbucket");
        //objectMetadata("villanova-csc9010-rvempati-testbucket","versioncheck");
        //putObjectWithExpiration("villanova-csc9010-rvempati-testbucket");
        int choice;
        do
        {
            
            
            System.out.println(" 1. PUT Object \n 2. GET Object \n 3. DELETE Object \n 4. PUT Bucket \n "
                    + "5. DELETE Bucket \n 6. HEAD Bucket \n 7. PUT - Copy Object \n 8. GET Service \n "
                    + "9. PUT Object with Expiration Date \n 10. PUT Bucket Versioning \n "
                    + "11. PUT Object with versioning \n 12. DELETE specific Object Version \n 13. HEAD Object \n 14. Exit Program");
            
            
            System.out.println("Enter your choice(1- 14) : ");
            Scanner input =new Scanner(System.in);
             choice=input.nextInt();
            
             String bucket="villanova-csc9010-rvempati-testbucket";
             
            switch(choice){
            
            case 1: putObject(bucket);
                    break;
            case 2: 
                    break;
            case 3: deleteObject(bucket, "test");
                    break;
            case 4: 
                    break;  
            case 5: 
                    break;
            case 6: 
                    break;  
            case 7: 
                    break;
            case 8: 
                    break;
            case 9: 
                    break;
            case 10: 
                    break;
            case 11: 
                    break;
            case 12: 
                    break;
            case 13: 
                    break;
            case 14: 
                    break;  
            default :System.out.println("Invalid choice ");
                    break;
            
            }
            
        }
        while(choice<=13);
        
        
        
        }
        catch(AmazonServiceException ase)
        {
            System.out.println(ase.getMessage());
        }
        
    }

}
