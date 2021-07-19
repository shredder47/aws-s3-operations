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

    public static synchronized S3Helper getInstance() {
        if (awsHelper == null) {
            awsHelper = new S3Helper();
        }
        return awsHelper;
    }


    // Create a bucket by using a S3Waiter object

    /**
     * Basically this is a create function through we are creating a bucket in the aws s3. In bucket everything
     * will be stored so it is necessary to create bucket. In this function first we are creating the object then
     * by the help of that object we are creating the bucket. Everything is put under try catch block so that if any
     * exception occurs it will display the message. Chance of occurring the error is when the bucket is already
     * there with the same name. Other errors can also occur.The name of the bucket is bucketName
     * which is in the 67 line number.
     * @param bucketName cbnits
     */
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


            // Wait until the bucket is created and print out the response

            /**
             * WaiterResponse is a library through which it waits for the bucket to be created. As soon as
             * bucket is created it will display the message bucket is ready.
             */
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println(bucketName + " is ready");

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    /**
     * This function prints the name of all the buckets present in the s3 storage.
     */
    public void listAllBuckets() {
        ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse listBucketsResponse = awsClient.listBuckets(listBucketsRequest);
        listBucketsResponse
                .buckets()
                .forEach(res -> System.out.println(res.name()));
    }

    /**
     * This function deletes all the empty buckets and it is put under try catch block because it may throw an error.
     * @param bucketName cbnits
     */
    public void deleteEmptyBucket(String bucketName) {
        try {
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            awsClient.deleteBucket(deleteBucketRequest);
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }

    }

    /**
     * This function deletes the item from the bucket. Item name is matched with the stored item name, if matched
     * successfully then item is deleted otherwise displays error message.
     * @param bucketName cbnits
     * @param itemName springmap
     */

    public void deleteItemFromBucket(String bucketName, String itemName) {

        List<ObjectIdentifier> deleteItems = new ArrayList<>();

        deleteItems.add(
                ObjectIdentifier
                        .builder()
                        .key(itemName)
                        .build()
        );

        try {
            Delete deleteInfo = Delete.builder().objects(deleteItems).build();

            DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(deleteInfo)
                    .build();

            awsClient.deleteObjects(deleteObjectsRequest);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        System.out.println("Deleting Successful");
    }

    /**
     * This function uploads item to the bucket. The file is ultimately stored in the given path name.
     * @param bucketName aditya123
     * @param keyOrPath /folder1/
     * @param sourceFilePath D:\Spring apps\demo
     * @throws IOException
     */


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

    /**
     * This function returns url of the file stored in a bucket. Here request and url is a object. GetUrlRequest
     * is a built in library.
     * @param bucketName aditya123
     * @param keyOrPath /folder1/
     * @param fileName chotu98
     * @return
     */
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

    /**
     * This function downloads the file from the bucket. If the path is empty then it will print invalid file
     * storage directory.
     * @param bucketName aditya123
     * @param keyOrPath /folder1/
     * @param downloadFileName springmap
     * @param outputFileDir D:\Download
     * @return
     */

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

    /**
     * This function will just result boolean value that is true or false. If the file exists in the bucket
     * then it will print true otherwise false.
     * @param bucketName cbnits
     * @param keyOrPath /folder1/
     * @param fileName springmap
     * @return
     */

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

    /**
     * This function will also result boolean value but this time for the bucket. If the bucket exists then true
     * otherwise false.
     * @param bucketName cbnits
     * @return
     */

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
