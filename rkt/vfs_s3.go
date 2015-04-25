package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"

	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/s3"
)

const REGION_US_EAST_1 = "us-east-1"

// PoC fetcher for S3
type vfsS3 struct {
}

// getS3 builds the s3 client for the specified region (or us-east-1 if region == "")
func (v *vfsS3) getS3(region string) *s3.S3 {
	// TODO: Try anonymous credentials?
	// TODO: Allow override of credentials for rkt?
	// TODO: Allow specifying DisableSSL? (only to be used when downloading full images, I think. Definitely not for keys!)
	// TODO: Cache services?

	awsConfig := &aws.Config{}
	awsConfig.Region = region
	svc := s3.New(awsConfig)
	return svc
}

// getBucketRegion determines the region where a bucket is located
func (v *vfsS3) getBucketRegion(bucketName string) (string, error) {
	// Start in us-east-1; we can find the correct location for the bucket from there
	svc := v.getS3(REGION_US_EAST_1)
	bucketLocationRequest := &s3.GetBucketLocationInput{
		Bucket: &bucketName,
	}
	bucketLocationResponse, err := svc.GetBucketLocation(bucketLocationRequest)
	if err != nil {
		return "", fmt.Errorf("error getting bucket location: %v", err)
	}

	if bucketLocationResponse.LocationConstraint == nil {
		return REGION_US_EAST_1, nil
	}

	return *bucketLocationResponse.LocationConstraint, nil
}

// downloadKey retrieves the file, storing it in a deleted tempfile
func (v *vfsS3) downloadKey(u *url.URL) (*os.File, error) {
	tf, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, fmt.Errorf("error creating tempfile: %v", err)
	}
	os.Remove(tf.Name()) // no need to keep the tempfile around

	defer func() {
		if err != nil {
			tf.Close()
		}
	}()

	bucketName := u.Host
	region, err := v.getBucketRegion(bucketName)
	if err != nil {
		return nil, err
	}

	svc := v.getS3(region)

	objectRequest := &s3.GetObjectInput{}
	objectRequest.Bucket = &bucketName
	objectRequest.Key = &u.Path

	objectResponse, err := svc.GetObject(objectRequest)
	if err != nil {
		return nil, fmt.Errorf("error getting key: %v", err)
	}
	defer objectResponse.Body.Close()

	if _, err := io.Copy(tf, objectResponse.Body); err != nil {
		return nil, fmt.Errorf("error copying key: %v", err)
	}

	tf.Seek(0, os.SEEK_SET)

	return tf, nil
}
