package main

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

	dataMap[DataFieldNameSdkType] = SdkType
	dataMap[DataFieldNameSdkVersion] = SdkVersion
	dataMap[DataFieldNameTime] = m.ReportTime

	logId, err := GenerateLogId()
	if err != nil {
		return nil, err
	}
	dataMap[DataFieldNameLogId] = logId

	dataMap[DataFieldNameOperate] = m.Operate
	dataMap[DataFieldNameIdentify] = m.Identity
	dataMap[DataFieldNameProperties] = m.Props

	return map[string]interface{}{
		"type": m.Type,
		"data": dataMap,
	}, nil
}
