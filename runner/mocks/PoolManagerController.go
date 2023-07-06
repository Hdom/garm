// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package mocks

import (
	context "context"

	common "github.com/cloudbase/garm/runner/common"

	databasecommon "github.com/cloudbase/garm/database/common"

	mock "github.com/stretchr/testify/mock"

	params "github.com/cloudbase/garm/params"
)

// PoolManagerController is an autogenerated mock type for the PoolManagerController type
type PoolManagerController struct {
	mock.Mock
}

// CreateEnterprisePoolManager provides a mock function with given fields: ctx, enterprise, providers, store
func (_m *PoolManagerController) CreateEnterprisePoolManager(ctx context.Context, enterprise params.Enterprise, providers map[string]common.Provider, store databasecommon.Store) (common.PoolManager, error) {
	ret := _m.Called(ctx, enterprise, providers, store)

	var r0 common.PoolManager
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, params.Enterprise, map[string]common.Provider, databasecommon.Store) (common.PoolManager, error)); ok {
		return rf(ctx, enterprise, providers, store)
	}
	if rf, ok := ret.Get(0).(func(context.Context, params.Enterprise, map[string]common.Provider, databasecommon.Store) common.PoolManager); ok {
		r0 = rf(ctx, enterprise, providers, store)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.PoolManager)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, params.Enterprise, map[string]common.Provider, databasecommon.Store) error); ok {
		r1 = rf(ctx, enterprise, providers, store)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CreateOrgPoolManager provides a mock function with given fields: ctx, org, providers, store
func (_m *PoolManagerController) CreateOrgPoolManager(ctx context.Context, org params.Organization, providers map[string]common.Provider, store databasecommon.Store) (common.PoolManager, error) {
	ret := _m.Called(ctx, org, providers, store)

	var r0 common.PoolManager
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, params.Organization, map[string]common.Provider, databasecommon.Store) (common.PoolManager, error)); ok {
		return rf(ctx, org, providers, store)
	}
	if rf, ok := ret.Get(0).(func(context.Context, params.Organization, map[string]common.Provider, databasecommon.Store) common.PoolManager); ok {
		r0 = rf(ctx, org, providers, store)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.PoolManager)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, params.Organization, map[string]common.Provider, databasecommon.Store) error); ok {
		r1 = rf(ctx, org, providers, store)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CreateRepoPoolManager provides a mock function with given fields: ctx, repo, providers, store
func (_m *PoolManagerController) CreateRepoPoolManager(ctx context.Context, repo params.Repository, providers map[string]common.Provider, store databasecommon.Store) (common.PoolManager, error) {
	ret := _m.Called(ctx, repo, providers, store)

	var r0 common.PoolManager
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, params.Repository, map[string]common.Provider, databasecommon.Store) (common.PoolManager, error)); ok {
		return rf(ctx, repo, providers, store)
	}
	if rf, ok := ret.Get(0).(func(context.Context, params.Repository, map[string]common.Provider, databasecommon.Store) common.PoolManager); ok {
		r0 = rf(ctx, repo, providers, store)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.PoolManager)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, params.Repository, map[string]common.Provider, databasecommon.Store) error); ok {
		r1 = rf(ctx, repo, providers, store)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DeleteEnterprisePoolManager provides a mock function with given fields: enterprise
func (_m *PoolManagerController) DeleteEnterprisePoolManager(enterprise params.Enterprise) error {
	ret := _m.Called(enterprise)

	var r0 error
	if rf, ok := ret.Get(0).(func(params.Enterprise) error); ok {
		r0 = rf(enterprise)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteOrgPoolManager provides a mock function with given fields: org
func (_m *PoolManagerController) DeleteOrgPoolManager(org params.Organization) error {
	ret := _m.Called(org)

	var r0 error
	if rf, ok := ret.Get(0).(func(params.Organization) error); ok {
		r0 = rf(org)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteRepoPoolManager provides a mock function with given fields: repo
func (_m *PoolManagerController) DeleteRepoPoolManager(repo params.Repository) error {
	ret := _m.Called(repo)

	var r0 error
	if rf, ok := ret.Get(0).(func(params.Repository) error); ok {
		r0 = rf(repo)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetEnterprisePoolManager provides a mock function with given fields: enterprise
func (_m *PoolManagerController) GetEnterprisePoolManager(enterprise params.Enterprise) (common.PoolManager, error) {
	ret := _m.Called(enterprise)

	var r0 common.PoolManager
	var r1 error
	if rf, ok := ret.Get(0).(func(params.Enterprise) (common.PoolManager, error)); ok {
		return rf(enterprise)
	}
	if rf, ok := ret.Get(0).(func(params.Enterprise) common.PoolManager); ok {
		r0 = rf(enterprise)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.PoolManager)
		}
	}

	if rf, ok := ret.Get(1).(func(params.Enterprise) error); ok {
		r1 = rf(enterprise)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetEnterprisePoolManagers provides a mock function with given fields:
func (_m *PoolManagerController) GetEnterprisePoolManagers() (map[string]common.PoolManager, error) {
	ret := _m.Called()

	var r0 map[string]common.PoolManager
	var r1 error
	if rf, ok := ret.Get(0).(func() (map[string]common.PoolManager, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[string]common.PoolManager); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]common.PoolManager)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetOrgPoolManager provides a mock function with given fields: org
func (_m *PoolManagerController) GetOrgPoolManager(org params.Organization) (common.PoolManager, error) {
	ret := _m.Called(org)

	var r0 common.PoolManager
	var r1 error
	if rf, ok := ret.Get(0).(func(params.Organization) (common.PoolManager, error)); ok {
		return rf(org)
	}
	if rf, ok := ret.Get(0).(func(params.Organization) common.PoolManager); ok {
		r0 = rf(org)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.PoolManager)
		}
	}

	if rf, ok := ret.Get(1).(func(params.Organization) error); ok {
		r1 = rf(org)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetOrgPoolManagers provides a mock function with given fields:
func (_m *PoolManagerController) GetOrgPoolManagers() (map[string]common.PoolManager, error) {
	ret := _m.Called()

	var r0 map[string]common.PoolManager
	var r1 error
	if rf, ok := ret.Get(0).(func() (map[string]common.PoolManager, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[string]common.PoolManager); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]common.PoolManager)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetRepoPoolManager provides a mock function with given fields: repo
func (_m *PoolManagerController) GetRepoPoolManager(repo params.Repository) (common.PoolManager, error) {
	ret := _m.Called(repo)

	var r0 common.PoolManager
	var r1 error
	if rf, ok := ret.Get(0).(func(params.Repository) (common.PoolManager, error)); ok {
		return rf(repo)
	}
	if rf, ok := ret.Get(0).(func(params.Repository) common.PoolManager); ok {
		r0 = rf(repo)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.PoolManager)
		}
	}

	if rf, ok := ret.Get(1).(func(params.Repository) error); ok {
		r1 = rf(repo)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetRepoPoolManagers provides a mock function with given fields:
func (_m *PoolManagerController) GetRepoPoolManagers() (map[string]common.PoolManager, error) {
	ret := _m.Called()

	var r0 map[string]common.PoolManager
	var r1 error
	if rf, ok := ret.Get(0).(func() (map[string]common.PoolManager, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() map[string]common.PoolManager); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]common.PoolManager)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateEnterprisePoolManager provides a mock function with given fields: ctx, enterprise
func (_m *PoolManagerController) UpdateEnterprisePoolManager(ctx context.Context, enterprise params.Enterprise) (common.PoolManager, error) {
	ret := _m.Called(ctx, enterprise)

	var r0 common.PoolManager
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, params.Enterprise) (common.PoolManager, error)); ok {
		return rf(ctx, enterprise)
	}
	if rf, ok := ret.Get(0).(func(context.Context, params.Enterprise) common.PoolManager); ok {
		r0 = rf(ctx, enterprise)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.PoolManager)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, params.Enterprise) error); ok {
		r1 = rf(ctx, enterprise)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateOrgPoolManager provides a mock function with given fields: ctx, org
func (_m *PoolManagerController) UpdateOrgPoolManager(ctx context.Context, org params.Organization) (common.PoolManager, error) {
	ret := _m.Called(ctx, org)

	var r0 common.PoolManager
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, params.Organization) (common.PoolManager, error)); ok {
		return rf(ctx, org)
	}
	if rf, ok := ret.Get(0).(func(context.Context, params.Organization) common.PoolManager); ok {
		r0 = rf(ctx, org)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.PoolManager)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, params.Organization) error); ok {
		r1 = rf(ctx, org)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateRepoPoolManager provides a mock function with given fields: ctx, repo
func (_m *PoolManagerController) UpdateRepoPoolManager(ctx context.Context, repo params.Repository) (common.PoolManager, error) {
	ret := _m.Called(ctx, repo)

	var r0 common.PoolManager
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, params.Repository) (common.PoolManager, error)); ok {
		return rf(ctx, repo)
	}
	if rf, ok := ret.Get(0).(func(context.Context, params.Repository) common.PoolManager); ok {
		r0 = rf(ctx, repo)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.PoolManager)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, params.Repository) error); ok {
		r1 = rf(ctx, repo)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewPoolManagerController creates a new instance of PoolManagerController. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewPoolManagerController(t interface {
	mock.TestingT
	Cleanup(func())
}) *PoolManagerController {
	mock := &PoolManagerController{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
