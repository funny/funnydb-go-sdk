package internal

type Reportable interface {
	transformToReportableData() (map[string]interface{}, error)
}
