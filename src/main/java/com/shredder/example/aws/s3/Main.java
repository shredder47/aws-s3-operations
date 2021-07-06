package com.shredder.example.aws.s3;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {

        S3Helper s3Helper = S3Helper.getInstance();

//        s3Helper.listAllBuckets();


//        s3Helper.uploadItemToBucket("nabo-user-images","group-images/","C:\\Users\\shred\\Downloads\\sample.pdf");
//        s3Helper.uploadItemToBucket("nabo-user-images","group-images/","Hello.pdf","C:\\Users\\shred\\Downloads\\sample.pdf");
        System.out.println(s3Helper.downloadItemFromBucket("nabo-user-images", "group-images/", "Hello.pdf", "C:\\Users\\shred\\Downloads\\"));
        System.out.println(s3Helper.getURL("nabo-user-images", "group-images/", "Hello.pdf"));


    }
}
