# Model notes

unions: map[object_type/relation_type] -> []string{ "object_type/relation_type" }
permissions: map[object_type/relation_type/permission] struct{}

pass 1: create unions map
pass 2: expand permission paths for all unions

map key values are:
* minimal 3 characters
* all lowercase ASCII characters
* the first character must be an alpha character,
* the second, to 63 character must be a alpha, digit, underscore or dash
* the last character must be a alpha or digit
* regex `^[a-z][a-z0-9_-]{1,62}[a-z0-9]$` 

