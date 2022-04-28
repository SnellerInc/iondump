# ION Dump Tool

ION Dump is a lightweight tool to dump [sneller](https://github.com/SnellerInc/sneller) `.ion.zst` files into human readable `JSON` text. 

The tool also serves as an example and demonstrates how to stream an `ion.zst` object from any S3-compatible storage, decompress it and finally convert its content to `JSON`. It consists of less than 300 lines of code and uses only publicly available third-party modules. 

## Usage

### Requirements:

The AWS credentials file (`~/.aws/credentials`) must be present and correctly [configured](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

### Example usage:

```bash
./iondump -e s3.us-east-1.amazonaws.com -f bucket/db/path/to/object.ion.zst
```

- `-e` Endpoint
- `-f` Bucket / path to object

The resulting `JSON` is written to `stdout`.

## Contribute

Sneller ION Dump is released under the Apache 2.0 license. See the LICENSE file for more information. 
