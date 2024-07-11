package funnydb

type M = map[string]interface{}

type reportable interface {
	transformToReportableData() (M, error)
}
