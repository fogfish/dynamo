//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package dynamo

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

/*

Optimistic Locking is a lightweight approach to ensure causal ordering of
read, write operations to database.Please check AWS document about
"Optimistic Locking with Version Number" and its implementation with AWS Java SDK.
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.OptimisticLocking.html

AWS DynamoDB support set of condition expression while doing I/O
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html

Conditional expressions are implemented using simple langauge
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html

*/

/*

TypeOf declares type descriptor to express I/O conditions.

Let's consider a following example:

  type Person struct {
    curie.ID
    Name    string `dynamodbav:"anothername,omitempty"`
  }

How to define a condition expression on the field Name? Golang struct defines and
refers the field by `Name` but DynamoDB stores it under the attribute `anothername`.
Struct field dynamodbav tag specifies serialization rules. Golang does not support a typesafe
approach to build a correspondence between `Name` ⟷ `anothername`. Developers have to
utilize dynamodb attribute name(s) in conditional expression and Golang struct name in
rest of the code. It becomes confusing and hard to maintain.

The types TypeOf and ElemIs are helper types to declare builders for conditional expressions
Just declare a global variables next to type definition and use them across the application.

  var name = Thing(Person{}).Field("Name")

*/
type TypeOf struct{ reflect.Type }

/*

ElemIs a conditional expressions builder for given attributed value.
*/
type ElemIs struct{ string }

/*

Constrain is a function that applies conditional expression to the DynamoDb request
*/
type Constrain func(
	conditionExpression **string,
	expressionAttributeNames map[string]*string,
	expressionAttributeValues map[string]*dynamodb.AttributeValue,
)

/*

Kind constructs type descriptor to express I/O conditions.
See TypeOf documentation
  var name = Thing(Person{}).Field("Name")
*/
func Kind(val interface{}) TypeOf {
	typeof := reflect.TypeOf(val)
	if typeof.Kind() == reflect.Ptr {
		typeof = typeof.Elem()
	}
	return TypeOf{typeof}
}

/*

Field constructs type descriptor to express I/O conditions.
See TypeOf documentation
  var name = Thing(Person{}).Field("Name")
*/
func (typeof TypeOf) Field(field string) ElemIs {
	spec, exists := typeof.FieldByName(field)
	if !exists {
		return ElemIs{""}
	}

	tag := spec.Tag.Get("dynamodbav")
	if tag == "" {
		return ElemIs{""}
	}

	return ElemIs{strings.Split(tag, ",")[0]}
}

/*

Eq is equal constrain
  name.Eq(x) ⟼ Field = :value
*/
func (e ElemIs) Eq(val interface{}) Constrain {
	return e.compare("=", val)
}

/*

Ne is non equal constrain
  name.Ne(x) ⟼ Field <> :value
*/
func (e ElemIs) Ne(val interface{}) Constrain {
	return e.compare("<>", val)
}

/*

Lt is less than constain
  name.Lt(x) ⟼ Field < :value
*/
func (e ElemIs) Lt(val interface{}) Constrain {
	return e.compare("<", val)
}

/*

Le is less or equal constain
  name.Le(x) ⟼ Field <= :value
*/
func (e ElemIs) Le(val interface{}) Constrain {
	return e.compare("<=", val)
}

/*

Gt is greater than constrain
  name.Le(x) ⟼ Field > :value
*/
func (e ElemIs) Gt(val interface{}) Constrain {
	return e.compare(">", val)
}

/*

Ge is greater or equal constrain
  name.Le(x) ⟼ Field >= :value
*/
func (e ElemIs) Ge(val interface{}) Constrain {
	return e.compare(">=", val)
}

/*

Is matches either Eq or NotExists if value is not defined
*/
func (e ElemIs) Is(val string) Constrain {
	if val == "_" {
		return e.NotExists()
	}

	return e.Eq(val)
}

func (e ElemIs) compare(fn string, val interface{}) Constrain {
	return func(
		conditionExpression **string,
		expressionAttributeNames map[string]*string,
		expressionAttributeValues map[string]*dynamodb.AttributeValue,
	) {
		if e.string == "" {
			return
		}

		lit, err := dynamodbattribute.Marshal(val)
		if err != nil {
			return
		}

		let := fmt.Sprintf(":__%s__", e.string)
		expressionAttributeValues[let] = lit
		*conditionExpression = aws.String(e.string + " " + fn + " " + let)
		return
	}
}

/*

Exists attribute constrain
  name.Exists(x) ⟼ attribute_exists(name)
*/
func (e ElemIs) Exists() Constrain {
	return e.constrain("attribute_exists")
}

/*

NotExists attribute constrain
	name.NotExists(x) ⟼ attribute_not_exists(name)
*/
func (e ElemIs) NotExists() Constrain {
	return e.constrain("attribute_not_exists")
}

func (e ElemIs) constrain(fn string) Constrain {
	return func(
		conditionExpression **string,
		expressionAttributeNames map[string]*string,
		expressionAttributeValues map[string]*dynamodb.AttributeValue,
	) {
		if e.string == "" {
			return
		}

		key := fmt.Sprintf("#__%s__", e.string)
		expressionAttributeNames[key] = aws.String(e.string)

		*conditionExpression = aws.String(fmt.Sprintf("%s(%s)", fn, key))
		return
	}
}
