package mirage

import (
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/labstack/echo/v4"
)

func (m *Mirage) handleGetDidFromHandle(e echo.Context) error {
	handle := e.Param("handle")

	_, err := syntax.ParseHandle(handle)
	if err != nil {
		return e.JSON(400, map[string]string{"error": "invalid handle"})
	}

	did, found, err := m.GetDidFromHandle(handle)
	if err != nil {
		return e.JSON(500, map[string]string{"error": err.Error()})
	}

	if !found {
		return e.JSON(404, map[string]string{"error": "handle not found in cache. it may exist, but we are not tracking it"})
	}

	return e.String(200, *did)
}

func (m *Mirage) handleGetHandleFromDid(e echo.Context) error {
	did := e.Param("did")

	_, err := syntax.ParseDID(did)
	if err != nil {
		return e.JSON(400, map[string]string{"error": "invalid did"})
	}

	handle, found, err := m.GetHandleFromDid(did)
	if err != nil {
		return e.JSON(500, map[string]string{"error": err.Error()})
	}

	if !found {
		return e.JSON(404, map[string]string{"error": "did not found"})
	}

	return e.String(200, *handle)
}

func (m *Mirage) handleResolveDid(e echo.Context) error {
	did := e.Param("didOrHandle")

	res, err := m.ResolveDid(did)
	if err != nil {
		return e.JSON(500, map[string]string{"error": err.Error()})
	}

	return e.JSON(200, res)
}

func (m *Mirage) handleGetPlcOpLog(e echo.Context) error {
	did := e.Param("didOrHandle")

	res, err := m.GetPlcOpLog(did)
	if err != nil {
		return e.JSON(500, map[string]string{"error": err.Error()})
	}

	if len(res) == 0 {
		return e.JSON(404, map[string]string{"error": "no plc op log found"})
	}

	ops := make([]interface{}, len(res))
	for i, op := range res {
		if op.Operation.PlcOperation != nil {
			ops[i] = op.Operation.PlcOperation
		} else if op.Operation.PlcTombstone != nil {
			ops[i] = op.Operation.PlcTombstone
		} else if op.Operation.LegacyPlcOperation != nil {
			ops[i] = op.Operation.LegacyPlcOperation
		}
	}

	return e.JSON(200, ops)
}

func (m *Mirage) handleGetLastOp(e echo.Context) error {
	did := e.Param("didOrHandle")

	res, err := m.GetLastOp(did)
	if err != nil {
		return e.JSON(500, map[string]string{"error": err.Error()})
	}

	if res == nil {
		return e.JSON(404, map[string]string{"error": "no op found"})
	}

	if res.Operation.PlcOperation != nil {
		return e.JSON(200, res.Operation.PlcOperation)
	} else if res.Operation.PlcTombstone != nil {
		return e.JSON(200, res.Operation.PlcTombstone)
	} else if res.Operation.LegacyPlcOperation != nil {
		return e.JSON(200, res.Operation.LegacyPlcOperation)
	}

	return e.JSON(500, map[string]string{"error": "unknown"})
}

func (m *Mirage) handleGetPlcData(e echo.Context) error {
	did := e.Param("didOrHandle")

	res, err := m.GetPlcData(did)
	if err != nil {
		return e.JSON(500, map[string]string{"error": err.Error()})
	}

	if res == nil {
		return e.JSON(404, map[string]string{"error": "no op found"})
	}

	return e.JSON(200, res)
}

func (m *Mirage) handleGetService(e echo.Context) error {
	did := e.Param("didOrHandle")

	res, found, err := m.GetService(did)
	if err != nil {
		return e.JSON(500, map[string]string{"error": err.Error()})
	}

	if !found {
		return e.JSON(404, map[string]string{"error": "no op found"})
	}

	return e.String(200, *res)
}

func (m *Mirage) handleExport(e echo.Context) error {
	return e.String(501, "this route is not implemented. to export the plc, use https://plc.directory/export")
}
