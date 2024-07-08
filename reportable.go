package main

type M = map[string]interface{}

type Reportable interface {
	TransformToReportableData() (M, error)
}
