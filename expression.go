package zsql

// Expression expression interface
type Expression interface {
	Build(builder Builder)
}
