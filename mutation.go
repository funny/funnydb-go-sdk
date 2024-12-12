package funnydb

import (
	"errors"
	"time"

	"github.com/funny/funnydb-go-sdk/v2/internal"
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
	dataMap[internal.DataFieldNameSdkType] = internal.SdkType
	dataMap[internal.DataFieldNameSdkVersion] = internal.SdkVersion

	if m.Time.IsZero() {
		m.Time = time.Now()
	}
	dataMap[internal.DataFieldNameTime] = m.Time.UnixMilli()

	logId, err := internal.GenerateLogId()
	if err != nil {
		return nil, err
	}
	dataMap[internal.DataFieldNameLogId] = logId

	dataMap[internal.DataFieldNameOperate] = m.Operate
	dataMap[internal.DataFieldNameIdentify] = m.Identity
	dataMap[internal.DataFieldNameProperties] = m.Props

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
