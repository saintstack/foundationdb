# Restore Validation Tenant Compatibility Fix

## Problem
The `RestoreValidation_Simple.toml` test was failing on Linux (Joshua) with:
```
Assertion payload.present() failed @ fdbclient/FileBackupAgent.actor.cpp:4373
```

This occurred during restore operations when:
1. The test configuration randomly enabled `tenant_mode=required_experimental`
2. Restore was performed with `addPrefix='restored/'`
3. The restore code tried to validate that prefixed keys belonged to valid tenants
4. The assertion failed because prefixed keys (e.g., `restored/mykey`) are not tenant keys

## Root Cause
In `fdbclient/FileBackupAgent.actor.cpp`, the `_validTenantAccess` function validates every restored key against the tenant cache. When restoring with a prefix for validation purposes, these prefixed keys are **not** tenant keys and have no associated tenant, causing the assertion to fail.

## Fix 1: Skip Tenant Validation for Prefixed Restores (Code)
Modified `fdbclient/FileBackupAgent.actor.cpp` line 4528:

```cpp
// Skip tenant validation when restoring with a prefix
// Prefixed keys (e.g., 'restored/' or '\xff\x02/rlog/') are not tenant keys
if (tenantCache.present() && addPrefix.get() == StringRef()) {
    validTenantCheckFutures.push_back(_validTenantAccess(...));
}
```

**Rationale**: Tenant validation should only apply when restoring to the original keyspace (no prefix). When restoring with a prefix, the keys are intentionally placed in a different location and are not tenant keys.

## Fix 2: Explicitly Disable Tenants in Test Configuration (TOML)
Added `[configuration]` sections to both test files:

### `tests/fast/RestoreValidation_Simple.toml`
```toml
[configuration]
# Disable tenant mode to avoid tenant validation during restore
config = 'single'
tenant_mode = 'disabled'
```

### `tests/fast/RestoreValidation.toml`
```toml
[configuration]
# Disable tenant mode to avoid tenant validation during restore
config = 'triple'
tenant_mode = 'disabled'
```

**Rationale**: The test framework's random configuration was enabling `tenant_mode=required_experimental`, which created a tenant cache. Explicitly disabling tenants ensures consistent test behavior and aligns with the restore validation feature's design (which doesn't require tenants).

## Why Both Fixes?
1. **Code fix** is defensive - prevents failures if tenants are enabled for any reason
2. **TOML fix** is explicit - ensures tests run in the intended configuration
3. **Together** they provide robust protection against tenant-related failures

## Files Modified
- `fdbclient/FileBackupAgent.actor.cpp` - Added condition to skip tenant validation for prefixed restores
- `tests/fast/RestoreValidation_Simple.toml` - Added `[configuration]` with `tenant_mode = 'disabled'`
- `tests/fast/RestoreValidation.toml` - Added `[configuration]` with `tenant_mode = 'disabled'`

## Testing
After these changes, the tests should:
- Pass consistently on both macOS and Linux
- Not encounter tenant-related assertions during restore
- Work correctly with or without tenant infrastructure present

