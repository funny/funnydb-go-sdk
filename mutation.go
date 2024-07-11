package funnydb

import (
	"errors"
	"time"
)

const (
	MutationTypeDevice = "DeviceMutation"
	MutationTypeUser   = "UserMutation"

	OperateTypeSet     = "set"
	OperateTypeSetOnce = "setOnce"
	OperateTypeAdd     = "add"
)

var MutationTypeIllegalError = errors.New("mutation type legal value is DeviceMutation or UserMutation")
var MutationDataOperateIllegalError = errors.New("mutation data operate legal value is set or setOnce or add")
var MutationDataIdentityIllegalError = errors.New("mutation data identity can not be empty")

type Mutation struct {
	Time     time.Time
	Type     string
	Identity string
	Operate  string
	Props    M
}

func (m *Mutation) transformToReportableData() (M, error) {
	dataMap := make(map[string]interface{})
	dataMap[dataFieldNameSdkType] = sdkType
	dataMap[dataFieldNameSdkVersion] = sdkVersion

	if m.Time.IsZero() {
		m.Time = time.Now()
	}
	dataMap[dataFieldNameTime] = m.Time.UnixMilli()

	logId, err := generateLogId()
	if err != nil {
		return nil, err
	}
	dataMap[dataFieldNameLogId] = logId

	dataMap[dataFieldNameOperate] = m.Operate
	dataMap[dataFieldNameIdentify] = m.Identity
	dataMap[dataFieldNameProperties] = m.Props

	return map[string]interface{}{
		"type": m.Type,
		"data": dataMap,
	}, nil
}

func (m *Mutation) checkData() error {
	switch m.Type {
	case MutationTypeDevice, MutationTypeUser:
	default:
		return MutationTypeIllegalError
	}

	switch m.Operate {
	case OperateTypeSet, OperateTypeSetOnce, OperateTypeAdd:
	default:
		return MutationDataOperateIllegalError
	}

	if m.Identity == "" {
		return MutationDataIdentityIllegalError
	}
	return nil
}
