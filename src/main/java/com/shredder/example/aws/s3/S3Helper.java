package com.shredder.example.aws.s3;

import com.shredder.example.aws.s3.util.PropertyReader;
import org.apache.commons.io.FileUtils;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;

public class S3Helper {

    private PropertyReader propertyReader;

    private static S3Helper awsHelper = null;
    private S3Client awsClient;

    /*instance of PropertyReader class is invoked
     * new instance of PropertyReader class will be created if encountered null
     * object for awsClient is created
     * storage created of region us-east-2
     * unusual exception will be handled in catch block*/
    private S3Helper() {
        try {

            propertyReader = PropertyReader.getInstance();
            Region region = Region.of(propertyReader.getProperty("aws.region"));

            awsClient = S3Client.builder()
                    .region(region)
                    .build();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    /*Object of S3Helper for the method getInstance will be created dynamically by invoking
     * S3Helper constructor if encountered as null and
     * the instance will be returned once created   */
    public static synchronized S3Helper getInstance() {
        if (awsHelper == null) {
            awsHelper = new S3Helper();
        }
        return awsHelper;
    }


    // Create a bucket by using a S3Waiter object
    public void createBucket(String bucketName) {

        try {
            S3Waiter s3Waiter = awsClient.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            awsClient.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();


            /* if the required bucket created or already present matches, the response of the bucket created will be displayed
             * else will wait until the bucket is created and will display the response once generated */
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println(bucketName + " is ready");

        }
        // display error message if exception incurred
        catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }
    //this method will print the names for each of the of buckets created in a list
    public void listAllBuckets() {
        ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse listBucketsResponse = awsClient.listBuckets(listBucketsRequest);
        listBucketsResponse
                .buckets()
                .forEach(res -> System.out.println(res.name()));
    }

    // deleteEmptyBucket method deletes the bucket(possibly empty) according to the bucket name passed as argument
    public void deleteEmptyBucket(String bucketName) {
        try {
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            awsClient.deleteBucket(deleteBucketRequest);
        }
        //handles exception if the bucket is not found with an error message
        catch (Exception exception) {
            System.err.println(exception.getMessage());
        }

    }
    /*List of stored items as intended are removed from the required bucket
     * (: mention proper item names to be identified)
     * (: mention proper bucket name)*/
    public void deleteItemFromBucket(String bucketName, String itemName) {

        List<ObjectIdentifier> deleteItems = new ArrayList<>();

        deleteItems.add(
                ObjectIdentifier
                        .builder()
                        .key(itemName)
                        .build()
        );
        //requires correct bucket name and item names to delete items
        try {
            Delete deleteInfo = Delete.builder().objects(deleteItems).build();

            DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(deleteInfo)
                    .build();

            awsClient.deleteObjects(deleteObjectsRequest);

        }
        // else incurs an error message for wrong details
        catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        System.out.println("Deleting Successful");
    }


    public void uploadItemToBucket(String bucketName, String keyOrPath, String sourceFilePath) throws IOException {
        Path path = Path.of(sourceFilePath);
        String newFileName = path.getFileName().toString();
        uploadItemToBucket(bucketName, keyOrPath, newFileName, sourceFilePath);
    }

    public void uploadItemToBucket(String bucketName, String keyOrPath, String newFileName, String sourceFilePath) throws IOException {

        Path path = Path.of(sourceFilePath);
        Map<String, String> metadata = new HashMap<>();

        metadata.put("file-type", "i2c Chained File");

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(keyOrPath.concat(newFileName))
                .metadata(metadata)
                .build();

        File file = new File(sourceFilePath);

        byte[] fileDataBytes = FileUtils.readFileToByteArray(file);
        ByteBuffer byteBuffer = ByteBuffer.wrap(fileDataBytes);

        awsClient.putObject(objectRequest, RequestBody.fromByteBuffer(byteBuffer));

        System.out.println("File -> ".concat(path.getFileName().toString()).concat(" was uploaded successfully as ".concat(newFileName)));
    }

    public Optional<String> getURL(String bucketName, String keyOrPath, String fileName) {

        try {
            GetUrlRequest request = GetUrlRequest.builder()
                    .bucket(bucketName)
                    .key(keyOrPath.concat(fileName))
                    .build();

            URL url = awsClient.utilities().getUrl(request);
            System.out.println("The URL for  " + fileName + " is " + url);
            return Optional.of(url.toString());

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return Optional.empty();
        }
    }

    public Optional<String> downloadItemFromBucket(String bucketName, String keyOrPath, String downloadFileName, String outputFileDir) {

        if (!FileUtils.isDirectory(new File(outputFileDir))) {
            System.out.println("INVALID FILE STORAGE DIR");
            return Optional.empty();
        }

        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyOrPath.concat(downloadFileName))
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = awsClient.getObjectAsBytes(objectRequest);
            byte[] dataByteArr = objectBytes.asByteArray();
            File outputFile = new File(outputFileDir.concat(FileSystems.getDefault().getSeparator()).concat(downloadFileName));
            FileUtils.writeByteArrayToFile(outputFile, dataByteArr);


            System.out.println("File " + downloadFileName + " is downloaded and stored successfully at " + outputFileDir);
            return Optional.of(new File(outputFileDir + FileSystems.getDefault().getSeparator() + downloadFileName).getAbsolutePath());

        } catch (IOException ex) {
            ex.printStackTrace();
            return Optional.empty();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return Optional.empty();
        }
    }

    public boolean isFileExists(String bucketName, String keyOrPath, String fileName) {
        try {
            HeadObjectResponse headResponse = awsClient
                    .headObject(HeadObjectRequest.builder().bucket(bucketName).key(keyOrPath.concat(fileName)).build());
            System.out.println(headResponse);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    public boolean isBucketExists(String bucketName) {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();
        try {
            awsClient.headBucket(headBucketRequest);
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }
}
