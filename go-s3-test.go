package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	// "github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/time/rate"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func main() {
	var bucket, key string
	var timeout, _ = time.ParseDuration("100000s")
	var upload_path string
	flag.StringVar(&bucket, "b", "", "Bucket name.")
	flag.StringVar(&key, "k", "", "Object key name.")
	flag.StringVar(&upload_path, "p", "", "path to upload")
	flag.Parse()

	var file, _ = os.Open(upload_path)
	defer file.Close()

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("eu-west-1"),
	}))

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancelFn func()
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}
	// Ensure the context is canceled to prevent leaking.
	// See context package for more information, https://golang.org/pkg/context/
	if cancelFn != nil {
		defer cancelFn()
	}

	// Uploads the object to S3. The Context will interrupt the request if the
	// timeout expires.
	uploader := s3manager.NewUploader(sess)

	var _, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),       // Bucket to be used
		Key:    aws.String(key),          // Name of the file to be saved
		Body:   NewDiskLimitReader(file), // File
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			fmt.Fprintf(os.Stderr, "upload canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "failed to upload object, %v\n", err)
		}
		os.Exit(1)
	}

	fmt.Printf("successfully uploaded file to %s/%s\n", bucket, key)
}

// NewDiskLimitReader returns a reader that is rate limited by disk limiter
func NewDiskLimitReader(r io.Reader) io.Reader {
	var diskLimiter = rate.NewLimiter(rate.Limit(200000000), 200000000+8*8192)

	return NewReader(r, diskLimiter)
}

type Reader struct {
	reader  io.Reader
	limiter *rate.Limiter
}

func NewReader(reader io.Reader, limiter *rate.Limiter) *Reader {
	return &Reader{reader, limiter}
}

func (r *Reader) Read(buf []byte) (int, error) {
	end := len(buf)
	if r.limiter.Burst() < end {
		end = r.limiter.Burst()
	}
	n, err := r.reader.Read(buf[:end])

	err = r.limiter.WaitN(context.TODO(), n)
	return n, err
}
