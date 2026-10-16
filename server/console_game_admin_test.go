package server

import (
	"context"
	"github.com/heroiclabs/nakama/v3/console"
	"github.com/heroiclabs/nakama/v3/console/acl"
	"testing"
)

func TestGameAdminRPCPermissions(t *testing.T) {
	wallet := func() acl.Permission {
		return acl.NewPermission(console.AclResources_ACCOUNT_WALLET, acl.PermissionWrite)
	}
	grant := func() acl.Permission {
		return wallet().Compose(acl.NewPermission(console.AclResources_NOTIFICATION, acl.PermissionWrite))
	}
	for _, sample := range []struct {
		method     string
		permission acl.Permission
		allowed    bool
	}{
		{"admin/bank_items", acl.None(), false},
		{"admin/bank_items", acl.NewPermission(console.AclResources_STORAGE_DATA, acl.PermissionRead), true},
		{"admin/grant_resources", wallet(), false},
		{"admin/grant_resources", grant(), true},
		{"admin/purchase_correct", grant(), false},
		{"admin/purchase_correct", grant().Compose(acl.NewPermission(console.AclResources_IN_APP_PURCHASE, acl.PermissionWrite)), true},
		{"admin/purchase_verify", acl.NewPermission(console.AclResources_IN_APP_PURCHASE, acl.PermissionRead), true},
		{"admin/purchase_verify", acl.None(), false},
	} {
		ctx := context.WithValue(context.Background(), ctxConsoleUsernameKey{}, "operator")
		ctx = context.WithValue(ctx, ctxConsoleUserAclKey{}, sample.permission)
		actor, err := authorizeGameAdminRPC(ctx, sample.method)
		if (err == nil) != sample.allowed || (sample.allowed && actor != "operator") {
			t.Fatalf("permission mismatch for %s: %s %v", sample.method, actor, err)
		}
	}
	for _, ctx := range []context.Context{context.Background(), context.WithValue(context.Background(), ctxConsoleUserAclKey{}, acl.Admin())} {
		if _, err := authorizeGameAdminRPC(ctx, "admin/grant_resources"); err == nil {
			t.Fatal("untrusted actor accepted")
		}
	}
	if actor, err := authorizeGameAdminRPC(context.Background(), "other/rpc"); err != nil || actor != "" {
		t.Fatal("unrelated RPC behavior changed")
	}
}
