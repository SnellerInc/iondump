package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/amzn/ion-go/ion"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	dashe string // -e = endpoint
	dashf string // -e = filename (bucket & path-to-object)
)

func exit(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func init() {
	flag.StringVar(&dashe, "e", "", "bucket/path-to-object")
	flag.StringVar(&dashf, "f", "", "endpoint")
}

func main() {

	flag.Parse()
	if dashe == "" || dashf == "" {
		flag.Usage()
		os.Exit(1)
	}

	bucket, object := s3split(dashf)
	if bucket == "" {
		exit(errors.New("no valid bucket specified"))
	}
	if !strings.HasSuffix(object, ".ion.zst") && !strings.HasSuffix(object, ".10n.zst") {
		exit(errors.New("no valid '.ion.zst' object specified"))
	}

	// Initialize S3 client

	home, err := os.UserHomeDir()
	if err != nil {
		exit(err)
	}
	creds := credentials.NewFileAWSCredentials(filepath.Join(home, ".aws", "credentials"), "")

	client, err := minio.New(dashe, &minio.Options{
		Creds:  creds,
		Secure: true,
	})
	if err != nil {
		exit(err)
	}

	// Prepare object stream

	size, err := sizeWithoutTrailer(client, bucket, object)
	if err != nil {
		exit(err)
	}

	obj, err := client.GetObject(context.Background(), bucket, object, minio.GetObjectOptions{})
	if err != nil {
		exit(err)
	}
	defer obj.Close()

	inputWithoutTrailer := &io.LimitedReader{R: obj, N: size}
	inputWithBVM := newBVMReader(inputWithoutTrailer)

	// Process

	compReader, compWriter := io.Pipe()
	decompReader, decompWriter := io.Pipe()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		defer compWriter.Close()
		err := extract(inputWithBVM, compWriter)
		if err != nil {
			exit(err)
		}
	}()
	go func() {
		defer wg.Done()
		defer decompWriter.Close()
		err := decompress(compReader, decompWriter)
		if err != nil {
			exit(err)
		}
	}()
	go func() {
		defer wg.Done()
		err := dump(decompReader, os.Stdout)
		if err != nil {
			exit(err)
		}
	}()

	wg.Wait()
}

// --

/// The s3split function splits a S3 path into `bucket` and `object` portions
func s3split(name string) (string, string) {
	out := strings.TrimPrefix(name, "s3://")
	split := strings.IndexByte(out, '/')
	if split == -1 || split == len(out) {
		exit(fmt.Errorf("invalid s3 path spec %q", out))
	}
	bucket := out[:split]
	object := out[split+1:]
	return bucket, object
}

/// The sizeWithoutTrailer function returns the size of the requested object
/// excluding the size of the Sneller specific trailer and offset
func sizeWithoutTrailer(client *minio.Client, bucket string, object string) (int64, error) {

	// The Sneller 'ion.zst' format contains a trailer and a 4-byte offset pointing
	// to the beginning of this trailer

	obj, err := client.GetObject(context.Background(), bucket, object, minio.GetObjectOptions{})
	if err != nil {
		exit(err)
	}
	defer obj.Close()

	stat, err := obj.Stat()
	if err != nil {
		return -1, err
	}

	data := make([]byte, 4)

	_, err = obj.ReadAt(data, stat.Size-4)
	if err != nil && err != io.EOF {
		return -1, err

	}

	offset := binary.LittleEndian.Uint32(data)

	return stat.Size - int64(offset) - 4, nil
}

/// The extract function extracts all ION data chunks from the outer ION
//  container and writes them to the output stream
func extract(in io.Reader, out io.Writer) error {

	// The Sneller 'ion.zst' format stores multiple chunks of ION data in `blob`
	// values of the outer ION container

	r := ion.NewReader(in)
	for r.Next() {
		t := r.Type()
		if t != ion.BlobType {
			return errors.New("unexpected token type")
		}
		val, err := r.ByteValue()
		if err != nil {
			return err
		}
		_, err = out.Write(val)
		if err != nil {
			return err
		}
	}
	return nil
}

/// The decompress function decompresses the given input data and writes the
/// resulting bytes to the output stream
func decompress(in io.Reader, out io.Writer) error {
	dec, err := zstd.NewReader(in)
	if err != nil {
		return err
	}
	defer dec.Close()
	_, err = io.Copy(out, dec)
	if err != nil {
		return err
	}
	return nil
}

/// The dump function reads ION data from the given input and writes an
/// equivalent textual representation to the output stream
func dump(in io.Reader, out io.Writer) error {
	dec := ion.NewTextDecoder(in)
	enc := ion.NewTextEncoder(out)

	for {
		val, err := dec.Decode()
		if err == ion.ErrNoInput {
			break
		} else if err != nil {
			return err
		}
		if err = enc.Encode(val); err != nil {
			return err
		}
	}
	if err := enc.Finish(); err != nil {
		return err
	}
	return nil
}

// ---

var bvm = [...]byte{0xE0, 0x01, 0x00, 0xEA}

type bvmReader struct {
	r io.Reader
	n int
}

func (r *bvmReader) Read(p []byte) (n int, err error) {

	// The Sneller 'ion.zst' format does not prepend the ION BVM. We have to add it to
	// allow the `ion.TextDecoder` to detect binary input format

	if r.n < 4 {
		n := len(p)
		if n > 4-r.n {
			n = 4 - r.n
		}
		r.n += copy(p, bvm[r.n:r.n+n])
		if n == len(p) {
			return n, nil
		}
		nr, err := r.r.Read(p[n:])
		if err != nil {
			return 0, err
		}
		return n + nr, nil

	}

	return r.r.Read(p)
}

func newBVMReader(input io.Reader) *bvmReader {
	return &bvmReader{r: input, n: 0}
}
