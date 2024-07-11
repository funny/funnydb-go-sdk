package funnydb

type M = map[string]interface{}

type Reportable interface {
	transformToReportableData() (M, error)
}
