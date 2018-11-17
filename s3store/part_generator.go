package s3store

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/tus/tusd"
)

func generateParts(r io.Reader, segmentSize int64, outChan chan<- *os.File, errChan chan<- error) {
	defer close(outChan)

	for {
		partFile, err := ioutil.TempFile("", "tusd-s3-tmp-")
		if err != nil {
			errChan <- err
			return
		}
		defer os.Remove(partFile.Name())
		defer partFile.Close()

		limitedReader := io.LimitReader(r, segmentSize)
		n, err := io.Copy(partFile, limitedReader)
		if err != nil {
			errChan <- err
			return
		}
		// io.Copy does not return io.EOF, so we have to handle it differently.
		// If it's finished reading, it will always return (0, nil).
		if n == 0 {
			errChan <- nil
			return
		}

		if _, err = partFile.Seek(0, 0); err != nil {
			errChan <- err
			return
		}

		outChan <- partFile
	}
}

func (store S3Store) processParts(id string, fileInfo tusd.FileInfo, nextPartNum int64, fileChan <-chan *os.File, outChan chan<- int64, errChan chan<- error) {
	defer close(outChan)

	uploadId, multipartId := splitIds(id)
	currentOffset := fileInfo.Offset
	bytesWritten := int64(0)

	for partFile := range fileChan {
		stat, err := partFile.Stat()
		if err != nil {
			outChan <- bytesWritten
			errChan <- err
			return
		}
		partSize := stat.Size()

		isCompletePart := partSize >= store.MinPartSize
		isFinalPart := !fileInfo.SizeIsDeferred && (fileInfo.Size-currentOffset == partSize)
		if isCompletePart || isFinalPart {
			_, err := store.Service.UploadPart(&s3.UploadPartInput{
				Bucket:     aws.String(store.Bucket),
				Key:        store.keyWithPrefix(uploadId),
				UploadId:   aws.String(multipartId),
				PartNumber: aws.Int64(nextPartNum),
				Body:       partFile,
			})
			if err != nil {
				outChan <- bytesWritten
				errChan <- err
				return
			}

			currentOffset += partSize
			bytesWritten += partSize
			nextPartNum++
		} else {
			if err := store.putIncompletePartForUpload(uploadId, partFile); err != nil {
				outChan <- bytesWritten
				errChan <- err
				return
			}
			bytesWritten += partSize
			break
		}
	}

	outChan <- bytesWritten
	errChan <- nil
}
