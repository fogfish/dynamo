package constrain

// Note: hides structure of constrai

/*

Unary operation, applies over the key
*/
type Unary struct {
	Op  string
	Key string
}

/*

Dyadic operation, applied over the key * value
*/
type Dyadic struct {
	Op  string
	Key string
	Val interface{}
}

/*

Eq is equal constrain
  name.Eq(x) ⟼ Field = :value
*/
func Eq[A any](key string, val A) *Dyadic {
	return &Dyadic{Op: "=", Key: key, Val: val}
}

/*

Ne is non equal constrain
  name.Ne(x) ⟼ Field <> :value
*/
func Ne[A any](key string, val A) *Dyadic {
	return &Dyadic{Op: "<>", Key: key, Val: val}
}

/*

Lt is less than constain
  name.Lt(x) ⟼ Field < :value
*/
func Lt[A any](key string, val A) *Dyadic {
	return &Dyadic{Op: "<", Key: key, Val: val}
}

/*

Le is less or equal constain
  name.Le(x) ⟼ Field <= :value
*/
func Le[A any](key string, val A) *Dyadic {
	return &Dyadic{Op: "<=", Key: key, Val: val}
}

/*

Gt is greater than constrain
  name.Le(x) ⟼ Field > :value
*/
func Gt[A any](key string, val A) *Dyadic {
	return &Dyadic{Op: ">", Key: key, Val: val}
}

/*

Ge is greater or equal constrain
  name.Le(x) ⟼ Field >= :value
*/
func Ge[A any](key string, val A) *Dyadic {
	return &Dyadic{Op: ">=", Key: key, Val: val}
}

/*

Is matches either Eq or NotExists if value is not defined
*/
func Is(key string, val string) interface{} {
	if val == "_" {
		return NotExists(key)
	}

	return Eq(key, val)
}

/*

Exists attribute constrain
  name.Exists(x) ⟼ attribute_exists(name)
*/
func Exists(key string) *Unary {
	return &Unary{Op: "attribute_exists", Key: key}
}

/*

NotExists attribute constrain
	name.NotExists(x) ⟼ attribute_not_exists(name)
*/
func NotExists(key string) *Unary {
	return &Unary{Op: "attribute_not_exists", Key: key}
}
