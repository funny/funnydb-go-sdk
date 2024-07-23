package funnydb

import (
	"errors"
	"git.sofunny.io/data-analysis/funnydb-go-sdk/internal"
	"time"
)

const (
	MutationTypeDevice = "DeviceMutation"
	MutationTypeUser   = "UserMutation"

	OperateTypeSet     = "set"
	OperateTypeSetOnce = "setOnce"
	OperateTypeAdd     = "add"
)

var ErrMutationTypeIllegal = errors.New("mutation type legal value is DeviceMutation or UserMutation")
var ErrMutationDataOperateIllegal = errors.New("mutation data operate legal value is set or setOnce or add")
var ErrMutationDataIdentityIllegal = errors.New("mutation data identity can not be empty")

type Mutation struct {
	Time     time.Time
	Type     string
	Identity string
	Operate  string
	Props    map[string]interface{}
}

func (m *Mutation) transformToReportableData() (map[string]interface{}, error) {
	dataMap := make(map[string]interface{})
	dataMap[dataFieldNameSdkType] = sdkType
	dataMap[dataFieldNameSdkVersion] = sdkVersion

	if m.Time.IsZero() {
		m.Time = time.Now()
	}
	dataMap[dataFieldNameTime] = m.Time.UnixMilli()

	logId, err := internal.GenerateLogId()
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
		return ErrMutationTypeIllegal
	}

	switch m.Operate {
	case OperateTypeSet, OperateTypeSetOnce, OperateTypeAdd:
	default:
		return ErrMutationDataOperateIllegal
	}

	if m.Identity == "" {
		return ErrMutationDataIdentityIllegal
	}
	return nil
}
