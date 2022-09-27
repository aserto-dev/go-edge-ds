# ToDo List

1. Implement ID generation when no-ID is specified
2. Adjust validation logic when no-ID is provided, currently this will fail validation, need to check if alternate key values are provided instead.
	* 	For an Int32 ID generator, investogate if can we use the BoltDB Sequance and SetSequence constructs to generate the value, which returns an uint64?
	* 	UUID ID generator, add ID.New() to the ID type
3. Unify types/* implementations right now this is a mix of instance type functions and type based functions
4. Verify hash consistency between the existing directory model which uses SQL Boiler models vs using the protobuf message representation.
	*	Especially for properties and schema (JSON objects)
5. Add ID.ValidIfSet() version

