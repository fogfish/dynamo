# dynamo example

Use following commands to evaluate example, access to AWS account is mandatory.

## KeyVal interface

```bash
cd keyval
go run main.go s3:///my-bucket
go run main.go ddb:///my-table
```

## Blob interface

```bash
cd blob
go run main.go s3:///my-bucket path/to/my/file.tgz
```

## Relational model

```bash
cd relational
sh schema.sh
go run main.go types.go
```