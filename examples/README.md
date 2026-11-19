# dynamo example

Use following commands to evaluate example, access to AWS account is mandatory.
Each example takes the name of DynamoDB table.

## KeyVal interface

```bash
cd keyval
go run main.go my-table
```

Other examples cover [batch I/O](batch), [conditional writes](conditional-writes),
[update expressions](update-expression) and [set types](update-expression-sets).

## Relational model

```bash
cd relational
sh schema.sh
go run main.go types.go
```
