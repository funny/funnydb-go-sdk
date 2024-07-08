package funnydb

import (
	"time"
)

const (
	MutationTypeDevice = "DeviceMutation"
	MutationTypeUser   = "UserMutation"

	OperateTypeSet     = "set"
	OperateTypeSetOnce = "setOnce"
	OperateTypeAdd     = "add"
)

type Mutation struct {
	ReportTime int64
	Type       string
	Identity   string
	Operate    string
	Props      M
}

func NewDeviceAddMutation(identity string, props map[string]interface{}) Mutation {
	return Mutation{
		ReportTime: time.Now().UnixMilli(),
		Type:       MutationTypeDevice,
		Operate:    OperateTypeAdd,
		Identity:   identity,
		Props:      props,
	}
}

func NewDeviceSetOnceMutation(identity string, props map[string]interface{}) Mutation {
	return Mutation{
		ReportTime: time.Now().UnixMilli(),
		Type:       MutationTypeDevice,
		Operate:    OperateTypeSetOnce,
		Identity:   identity,
		Props:      props,
	}
}

func NewDeviceSetMutation(identity string, props map[string]interface{}) Mutation {
	return Mutation{
		ReportTime: time.Now().UnixMilli(),
		Type:       MutationTypeDevice,
		Operate:    OperateTypeSet,
		Identity:   identity,
		Props:      props,
	}
}

func NewUserAddMutation(identity string, props map[string]interface{}) Mutation {
	return Mutation{
		ReportTime: time.Now().UnixMilli(),
		Type:       MutationTypeUser,
		Operate:    OperateTypeAdd,
		Identity:   identity,
		Props:      props,
	}
}

func NewUserSetOnceMutation(identity string, props map[string]interface{}) Mutation {
	return Mutation{
		ReportTime: time.Now().UnixMilli(),
		Type:       MutationTypeUser,
		Operate:    OperateTypeSetOnce,
		Identity:   identity,
		Props:      props,
	}
}

func NewUserSetMutation(identity string, props map[string]interface{}) Mutation {
	return Mutation{
		ReportTime: time.Now().UnixMilli(),
		Type:       MutationTypeUser,
		Operate:    OperateTypeSet,
		Identity:   identity,
		Props:      props,
	}
}

func (m *Mutation) TransformToReportableData() (M, error) {
	dataMap := make(map[string]interface{})

	dataMap[dataFieldNameSdkType] = sdkType
	dataMap[dataFieldNameSdkVersion] = sdkVersion
	dataMap[dataFieldNameTime] = m.ReportTime

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
